import moment from 'moment';
import mongoose from 'mongoose';

const Schema = mongoose.Schema;

export const DomainSchema = new Schema({
  customers: [{
    customerId: {
      type: Schema.Types.ObjectId,
      ref: 'Customer'
    },
    mailPreferences: {
      weeklyReport: {
        type: Boolean,
        default: true
      }
    },
    accessLevel: {
      type: String,
      default: 'creator',
      enum: ['creator', 'admin', 'member', 'viewer']
    }
  }],
  customer: {
    type: Schema.Types.ObjectId,
    ref: 'Customer'
  },
  company: {
    name: {
      type: String,
      required: true
    },
    website: {
      type: String,
      required: true
    }
  },
  apiKey: {
    type: String,
    required: true
  },
  customerDBName: {
    type: String,
    required: true
  },
  setup: {
    type: Boolean,
    default: false
  },
  integrations: {
    hubspot: {
      status: Boolean,
      accounts: [{
        hubId: String,
        hubDomain: String,
        accessToken: String,
        refreshToken: String,
        lastPulledDate: Date,
        lastPulledDates: {
          companies: {
            type: Date,
            default: moment().subtract(4, 'year').toISOString()
          },
          contacts: {
            type: Date,
            default: moment().subtract(4, 'year').toISOString()
          },
          deals: {
            type: Date,
            default: moment().subtract(4, 'year').toISOString()
          },
          meetings: {
            type: Date,
            default: moment().subtract(4, 'year').toISOString()
          }
        }
      }]
    }
  }
}, { minimize: false });

export const Domain = mongoose.model('Domain', DomainSchema);
