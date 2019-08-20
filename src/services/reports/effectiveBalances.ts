import { Container, Service } from 'typedi';

import { environment } from '../../environments/environment';
import { LoggerService } from '../logger.service';
import { BalanceController } from '../../controllers/balance';
import { AccountsBalanceHistory, IEffectiveBalance } from '../../models/balances';
import { ClientSession, BulkWriteOpResultObject } from 'mongodb';
import { Account, AccountType } from '../../models/accounts';
import { Types } from 'mongoose';
import moment from 'moment';
import { threadId } from 'worker_threads';

const {
  performance,
} = require('perf_hooks');


@Service()
export class EffectiveBalance {
  private logger: LoggerService = Container.get(LoggerService);
  // private balanceController: BalanceController = Container.get(BalanceController);

  constructor() {
   this.demon(24 * 60 * 60 * 1000);
  }

  get startDate () {
    return '2019-08-01';
  }

  delay = (t) => new Promise(resolve => setTimeout(resolve, t));

  async bodyAsyncForDemon () {
    this.logger.info(`Process is started`);
    const effectiveDates = await this.getEffectiveDates();
    // TODO: kak dobavlyaetsya accountsID?
    return await this.makeReport({dates: effectiveDates, ids: ['5be92db0e6687d38b2d33817']});
  }
  async demon(n, isNoError = true) {
    while (isNoError) {
      console.log(`Start proccess`);
      try {
        const result = await this.bodyAsyncForDemon();
        this.logger.info(`Success, next recalculation in ${n} mills`, { result: JSON.stringify(result) });
        await delay(n);
      } catch (error) {
        isNoError = false;
        this.logger.error(`error`, { error });
      }
    }
  }


  async getEffectiveDates() {
    const count = await AccountsBalanceHistory.find().countDocuments();
    if (count === 0) {
      return this.countEffectiveDates(new Date(this.startDate), new Date());
    } else {
      let latestDateInCollection = await this.getLatestEffectiveDateFromCollection();
      // TODO: esli nujen pererashet to minus odin den tobavit
      latestDateInCollection = latestDateInCollection.map(({effectiveDate}) => this.formatDate(effectiveDate));
      return this.countEffectiveDates(new Date(latestDateInCollection), new Date());
    }

  }

  async getLatestEffectiveDateFromCollection(): Promise<any> {
    return await AccountsBalanceHistory.aggregate([
      {
        '$match': {}
      },
      {
        '$sort': {
          'effectiveDate': -1
        }
      },
      {
        '$limit': 1
      }
    ]);
  }

  formatDate (d) {
    return moment(new Date(d)).format('YYYY-MM-DD');
  }
  private countEffectiveDates(startDate, stopDate) {

    let dateArray = new Array();
    let currentDate = startDate;
    while (currentDate <= stopDate) {
        dateArray = [...dateArray, this.formatDate(currentDate)];
        currentDate = new Date(currentDate).setDate(new Date(currentDate).getDate() + 1);
    }
    console.log('effectiveDates', dateArray);
    return dateArray;

  }


  async makeReport({ dates, ids }) {
    return this.fireEffectiveBalance({dates, ids});
  }

  async inserMany (
    balances: Array<IEffectiveBalance>,
    session?: ClientSession,
  ): Promise<IEffectiveBalance[]> {
    try {
      const options = { ordered: false, session };
      const output = await AccountsBalanceHistory.insertMany(
        balances.map(({_id, ...data}) => data),
        options,
      ) as any;
      console.log('SUCCESS inserMany', output);
      return output;
    } catch (error) {
      console.log('ERROR inserMany', error);
      this.logger.error({ error });
    }
  }

  balancesUpdate(balances: Array<IEffectiveBalance>): Array<any> {
    const timestamp = new Date();
    return balances
    .map(
        balance => {
          const {currency, accountId, effectiveDate, totalFromFiltered, totalToFiltered, effectiveBalance } = balance;
          return ({
            updateOne: {
              filter: { currency, accountId, effectiveDate },
              update: {
                $set: { updatedAt: timestamp, totalFromFiltered, totalToFiltered, effectiveBalance }
              },
            }
          });
        }
      );
  }


  async bulkWrite (
    balances: Array<IEffectiveBalance>,
    session?: ClientSession,
  ): Promise<BulkWriteOpResultObject> {
    try {
      const options = { ordered: false, session };
      const output = await AccountsBalanceHistory.collection.bulkWrite(this.balancesUpdate(balances), options);
      console.log('SUCCESS bulkWrite', output);
      return output;
    } catch (error) {
      console.log('ERROR bulkWrite', error);
      this.logger.error({ error });
    }
  }


  // return await Promise.all(list.map(item => anAsyncFunction(item)))
  async effectiveBalancesFn ({ids = [], aggregationQueries = []}): Promise<any[]> {
    return await Promise.all(aggregationQueries.map(
      aggregationQuery => (
        Account.aggregate(ids.length === 0 ?
          [
            ...aggregationQuery
          ] :
          [
            {$match: { _id: {$in : ids.map(id => new Types.ObjectId(id))} }},
            ...aggregationQuery
          ]
        )
      )
    )).reduce((acc, val) => acc.concat(val), []);
  }

  async fireEffectiveBalance ({dates, ids }): Promise<any> {

    const effectiveDates = dates.map(date => new Date(date));
    console.log('effectiveDates', effectiveDates);
    const before = performance.now();

    const aggregationQueries = effectiveDates.map(effectiveDate => [
      {
        '$addFields': {
          '_idstr': {
            '$toString': '$_id'
          }
        }
      }, {
        '$graphLookup': {
          'from': 'transactions',
          'startWith': '$_idstr',
          'connectFromField': 'amount',
          'connectToField': 'fromAccount',
          'as': 'amountsFrom'
        }
      }, {
        '$graphLookup': {
          'from': 'transactions',
          'startWith': '$_idstr',
          'connectFromField': 'amount',
          'connectToField': 'toAccount',
          'as': 'amountsTo',
          'depthField': 'depthField'
        }
      }, {
        '$project': {
          '_id': 1,
          'currency': 1,
          'pair': 1,
          'type': 1,
          'userID': 1,
          'balance': 1,
          'crypto': 1,
          'status': 1,
          'accountId': '$_idstr',
          '_effectiveDate': 1,
          'amountsFrom': 1,
          'amountsTo': 1,
          'amountsToFiltered': {
            '$filter': {
              'input': '$amountsTo',
              'as': 'amountTo',
              'cond': {
                '$lt': [
                  '$$amountTo.createdAt', effectiveDate
                ]
              }
            }
          },
          'amountsFromFiltered': {
            '$filter': {
              'input': '$amountsFrom',
              'as': 'amountFrom',
              'cond': {
                '$lt': [
                  '$$amountFrom.createdAt', effectiveDate
                ]
              }
            }
          }
        }
      }, {
        '$project': {
          '_id': 1,
          'currency': 1,
          'pair': 1,
          'type': 1,
          'crypto': 1,
          'status': 1,
          'accountId': 1,
          'effectiveDate': effectiveDate,
          'createdAt': '$Date()',
          'totalFromFiltered': {
            '$sum': '$amountsFromFiltered.amount'
          },
          'totalToFiltered': {
            '$sum': '$amountsToFiltered.amount'
          },
          'effectiveBalance': {
            '$subtract': [
              {
                '$sum': '$amountsToFiltered.amount'
              }, {
                '$sum': '$amountsFromFiltered.amount'
              }
            ]
          }
        }
      }
    ]);

    const effectiveBalances: Array<IEffectiveBalance> = await this.effectiveBalancesFn({ids, aggregationQueries});

    const after = performance.now();
    const mills = (after - before);

    console.log('effectiveBalances', effectiveBalances, 'mills', mills);

    const count = await AccountsBalanceHistory.find().countDocuments();
    let result;
    if (count === 0) {
      result = await this.inserMany(effectiveBalances);
    } else {
      result = await this.bulkWrite(effectiveBalances);
    // todo: voswrat dannih seichas prohodit
    //   "data": {
    //     "ok": 1,
    //     "writeErrors": [],
    //     "writeConcernErrors": [],
    //     "insertedIds": [],
    //     "nInserted": 0,
    //     "nUpserted": 0,
    //     "nMatched": 3,
    //     "nModified": 3,
    //     "nRemoved": 0,
    //     "upserted": [],
    //     "lastOp": {
    //         "ts": "6732412277871345667",
    //         "t": 7
    //     }
    // }
    }

    return result;
  }


  //   setTimeout(async () => {
  //     await this.rates.getNewRatesAndSave();
  //     this.updateRates();
  //     const arg = `Node.js`;
  //     // setTimeout((arg)=>console.log(arg), 2 * 1000, `${arg}`);
  //     const tick = (n, errorCount) => {
  //       console.log(`init every ${n} mills `)
  //       setTimeout(d => tick(d, errorCount), n * 1000, n*2);
  //     }
  //     tick(24*60*60, 0)//h/min/sec

  // }, environment.rateTime * 1000);

}
