
import { config } from 'dotenv';
import mongoose from 'mongoose';
import { HubSpotWorker } from './hubspot.worker.js';
config()

mongoose.set('strictQuery', false);
const MONGO_URI = process.env.MONGO_URI

function init() {

  mongoose.connect(MONGO_URI)

  console.log('Connected to the database');

  const worker = new HubSpotWorker()

  worker.runWorker()

}


//OPTIONAL: CAN COST MORE MONEY THAN JUST RUNNING THE SCRIPT, BECAUSE OF THE MEMORY USAGE ALONG THE TIME
// const RUN_JOB_AT_12_AM_EVERY_DAY = '0 0 * * *';
// const job = new CronJob(RUN_JOB_AT_12_AM_EVERY_DAY, async () => {
//     console.log('Running job at 12 AM every day');
//     const worker = new HubSpotWorker()
//     worker.run
// });
// job.start();

init()