import { queue as queueAsync } from 'async';
import { filterNullValuesFromObject } from "./common/utils/filterNullValuesFromObject.js";
import { Domain } from './schemas/domain.schema.js';
import hubspot from '@hubspot/api-client';

export class HubSpotWorker {

  hubspotClient = new hubspot.Client({ accessToken: '' });

  action = []

  queueObj = null;

  expirationTokenDate;

  async runWorker() {

    console.log('start pulling data from HubSpot');

    const domain = await Domain.findOne({});

    const accounts = domain?.integrations?.hubspot?.accounts;

    if (!accounts) throw new Error('No accounts found');

    for (const account of accounts) {
      console.log('start processing account');

      const queue = this._createQueue(domain, this.action);

      await this._refreshAccessToken(domain, account.hubId);

      const logError = (err, operation) => {
        console.log(err, { apiKey: domain.apiKey, metadata: { operation: operation, hubId: account.hubId } });
      }


      console.log('start processing contacts');

      await this._processContacts(domain, account.hubId, queue)
        .then(() => console.log('Contacts processed'))
        .catch((err) => logError(err, 'processContacts'))

      console.log('start processing companies');

      await this._processCompanies(domain, account.hubId, queue)
        .then(() => console.log('Companies processed'))
        .catch((err) => logError(err, 'processCompanies'))

      console.log('start processing meetings');
      await this._processMeetings(domain, account.hubId, queue)
        .then(() => console.log('Meetings processed'))
        .catch((err) => logError(err, 'processMeetings'))

      console.log('finish processing account');
    }

    process.exit(0);
  }



  async _processContacts(domain, hubId, queue) {
    const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
    const lastPulledDate = new Date(account.lastPulledDates.contacts);
    const now = new Date();

    let hasMore = true;
    const offsetObject = {};
    const limit = 100;

    while (hasMore) {
      const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
      const lastModifiedDateFilter = this._generateLastModifiedDateFilter(lastModifiedDate, now, 'lastmodifieddate');
      const searchObject = {
        filterGroups: [lastModifiedDateFilter],
        sorts: [{ propertyName: 'lastmodifieddate', direction: 'ASCENDING' }],
        properties: [
          'firstname',
          'lastname',
          'jobtitle',
          'email',
          'hubspotscore',
          'hs_lead_status',
          'hs_analytics_source',
          'hs_latest_source'
        ],
        limit,
        after: offsetObject.after
      };

      let searchResult = {};


      let tryCount = 0;
      while (tryCount <= 4) {
        try {
          searchResult = await this.hubspotClient.crm.contacts.searchApi.doSearch(searchObject);
          break;
        } catch (err) {
          tryCount++;
          if (new Date() > this.expirationTokenDate) await this._refreshAccessToken(domain, hubId);
          await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
        }
      }

      if (!searchResult) throw new Error('Failed to fetch contacts for the 4th time. Aborting.');

      const data = searchResult.results || [];

      console.log('fetch contact batch');

      offsetObject.after = parseInt(searchResult.paging?.next?.after);
      const contactIds = data.map(contact => contact.id);

      // contact to company association
      const contactsToAssociate = contactIds;
      const companyAssociationsResults = (await (await this.hubspotClient.apiRequest({
        method: 'post',
        path: '/crm/v3/associations/CONTACTS/COMPANIES/batch/read',
        body: { inputs: contactsToAssociate.map(contactId => ({ id: contactId })) }
      })).json())?.results || [];

      const companyAssociations = Object.fromEntries(companyAssociationsResults.map(a => {
        if (a.from) {
          contactsToAssociate.splice(contactsToAssociate.indexOf(a.from.id), 1);
          return [a.from.id, a.to[0].id];
        } else return false;
      }).filter(x => x));

      data.forEach(contact => {
        if (!contact.properties || !contact.properties.email) return;

        const companyId = companyAssociations[contact.id];

        const isCreated = new Date(contact.createdAt) > lastPulledDate;

        const userProperties = {
          company_id: companyId,
          contact_name: ((contact.properties.firstname || '') + ' ' + (contact.properties.lastname || '')).trim(),
          contact_title: contact.properties.jobtitle,
          contact_source: contact.properties.hs_analytics_source,
          contact_status: contact.properties.hs_lead_status,
          contact_score: parseInt(contact.properties.hubspotscore) || 0
        };

        const actionTemplate = {
          includeInAnalytics: 0,
          identity: contact.properties.email,
          userProperties: filterNullValuesFromObject(userProperties)
        };

        queue.push({
          actionName: isCreated ? 'Contact Created' : 'Contact Updated',
          actionDate: new Date(isCreated ? contact.createdAt : contact.updatedAt),
          ...actionTemplate
        });
      });

      if (!offsetObject?.after) {
        hasMore = false;
        break;
      } else if (offsetObject?.after >= 9900) {
        offsetObject.after = 0;
        offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
      }
    }

    account.lastPulledDates.contacts = now;
    await this._saveDomain(domain);

    return true;
  }

  async _processCompanies(domain, hubId, queue) {
    const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
    const lastPulledDate = new Date(account.lastPulledDates.companies);
    const now = new Date();

    let hasMore = true;
    const offsetObject = {};
    const limit = 100;

    while (hasMore) {
      const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
      const lastModifiedDateFilter = this._generateLastModifiedDateFilter(lastModifiedDate, now);
      const searchObject = {
        filterGroups: [lastModifiedDateFilter],
        sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING' }],
        properties: [
          'name',
          'domain',
          'country',
          'industry',
          'description',
          'annualrevenue',
          'numberofemployees',
          'hs_lead_status'
        ],
        limit,
        after: offsetObject.after
      };

      let searchResult = {};

      let tryCount = 0;
      while (tryCount <= 4) {
        try {
          searchResult = await this.hubspotClient.crm.companies.searchApi.doSearch(searchObject);
          break;
        } catch (err) {
          tryCount++;

          if (new Date() > this.expirationTokenDate) await this._refreshAccessToken(domain, hubId);

          await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
        }
      }

      if (!searchResult) throw new Error('Failed to fetch companies for the 4th time. Aborting.');

      const data = searchResult?.results || [];
      offsetObject.after = parseInt(searchResult?.paging?.next?.after);

      console.log('fetch company batch');

      data.forEach(company => {
        if (!company.properties) return;

        const actionTemplate = {
          includeInAnalytics: 0,
          companyProperties: {
            company_id: company.id,
            company_domain: company.properties.domain,
            company_industry: company.properties.industry
          }
        };

        const isCreated = !lastPulledDate || (new Date(company.createdAt) > lastPulledDate);

        queue.push({
          actionName: isCreated ? 'Company Created' : 'Company Updated',
          actionDate: new Date(isCreated ? company.createdAt : company.updatedAt) - 2000,
          ...actionTemplate
        });
      });

      if (!offsetObject?.after) {
        hasMore = false;
        break;
      } else if (offsetObject?.after >= 9900) {
        offsetObject.after = 0;
        offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
      }
    }

    account.lastPulledDates.companies = now;
    await this._saveDomain(domain);

    return true;
  }

  async _processMeetings(domain, hubId, queue) {
    const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
    const lastPulledDate = new Date(account.lastPulledDates.meetings);
    const now = new Date();

    let hasMore = true;
    const offsetObject = {};
    const limit = 100;

    while (hasMore) {
      const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
      const lastModifiedDateFilter = this._generateLastModifiedDateFilter(lastModifiedDate, now);

      const searchObject = {
        filterGroups: [lastModifiedDateFilter], //COMMENT IT TO FETCH ALL MEETINGS, IF NOT ABLE TO TEST
        sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING' }],
        limit: limit,
        after: offsetObject.after,
        properties: [
          'hs_meeting_title',
        ]
      };

      let searchResult = {};

      for (let i = 0; i < 4; i++) {
        try {
          searchResult = await this.hubspotClient.crm.objects.meetings.searchApi.doSearch(searchObject);
          break;
        }
        catch (err) {
          if (new Date() > this.expirationTokenDate) await this._refreshAccessToken(domain, hubId);
          await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, i)));
        }
      }

      if (!searchResult) throw new Error('Failed to fetch meetings for the 4th time. Aborting.');

      const data = searchResult.results || [];

      console.log('fetch meeting batch');

      offsetObject.after = parseInt(searchResult?.paging?.next?.after);

      for (const meeting of data) {

        const associationsResult = await this.hubspotClient.crm.objects.meetings.associationsApi.getAll(meeting.id, 'contact');
        const associationsIds = associationsResult.results.map(association => association.toObjectId);
        const contacts = []
        for (const contactId of associationsIds) {
          const contact = await this.hubspotClient.crm.contacts.basicApi.getById(contactId);
          contacts.push(contact)
        }

        const meetinProperties = {
          meeting_id: meeting.id,
          meeting_title: meeting.properties.hs_meeting_title,
          meeting_createdate: meeting.properties.hs_createdate,
          meeting_lastmodifieddate: meeting.properties.hs_lastmodifieddate,
          contacts_emails: contacts.map(contact => contact.properties.email),
        };

        const actionTemplate = {
          includeInAnalytics: 0,
          id: meeting.id,
          meetingProperties: filterNullValuesFromObject(meetinProperties)
        };

        const isCreated = !lastPulledDate || (new Date(meeting.createdAt) > lastPulledDate);

        queue.push({
          actionName: isCreated ? 'Meeting Created' : 'Meeting Updated',
          actionDate: new Date(isCreated ? meeting.createdAt : meeting.updatedAt) - 2000,
          ...actionTemplate
        });
      }

      if (!offsetObject?.after) {
        hasMore = false;
        break;
      } else if (offsetObject?.after >= 9900) {
        offsetObject.after = 0;
        offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
      }
    }

    account.lastPulledDates.meetings = now;
    await this._saveDomain(domain);

    return true;
  }

  async _refreshAccessToken(domain, hubId) {

    const { HUBSPOT_CID, HUBSPOT_CS } = process.env;
    const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
    const { refreshToken } = account;


    const res = await this.hubspotClient.oauth.tokensApi.createToken(
      'refresh_token', undefined, undefined, HUBSPOT_CID, HUBSPOT_CS, refreshToken)

    const body = res.body || res;

    const accessToken = body.accessToken;

    this.expirationTokenDate = new Date(body.expiresIn * 1000 + new Date().getTime());

    account['accessToken'] = accessToken;

    this.hubspotClient.setAccessToken(accessToken);

    return accessToken;
  }

  async _drainQueue(actions, q) {
    if (q.length() > 0) await q.drain();

    if (actions.length > 0) {
      goal(actions)
    }

    return true;
  }

  async _saveDomain(domain) {
    // disable this for testing purposes
    return;
    domain.markModified('integrations.hubspot.accounts');
    await domain.save();
  }


  _createQueue = (domain, actions) => queueAsync(async (action, callback) => {
    actions.push(action);

    if (actions.length > 2000) {
      console.log('inserting actions to database', { apiKey: domain.apiKey, count: actions.length });

      const copyOfActions = _.cloneDeep(actions);
      actions.splice(0, actions.length);

      this.saveActionsInDatabase(copyOfActions);
    }

    callback();
  }, 100000000);

  _saveActionsInDatabase = async (actions) => {
    // save actions in database
    console.log('actions saved in database', { count: actions.length });
  }

  _generateLastModifiedDateFilter(date, nowDate, propertyName = 'hs_lastmodifieddate') {
    const lastModifiedDateFilter = date ?
      {
        filters: [
          { propertyName, operator: 'GTE', value: `${date.valueOf()}` },
          { propertyName, operator: 'LTE', value: `${nowDate.valueOf()}` }
        ]
      } :
      {};

    return lastModifiedDateFilter;
  };

}