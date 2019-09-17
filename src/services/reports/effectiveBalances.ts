import { Container, Service } from 'typedi';

import { environment } from '../../environments/environment';
import { LoggerService } from '../logger.service';
import { AccountsBalanceHistory, IEffectiveBalance } from '../../models/balances';
import { ClientSession, BulkWriteOpResultObject } from 'mongodb';
import { Account, AccountType } from '../../models/accounts';
import { User } from '../../models/users';
import { Currency, ICurrency } from '../../models/currencies';
import { Types } from 'mongoose';
import moment from 'moment';

const {
  performance,
} = require('perf_hooks');


@Service()
export class EffectiveBalance {
  private logger: LoggerService = Container.get(LoggerService);

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
    // tslint:disable-next-line:max-line-length
    return await this.makeReport({dates: effectiveDates, ids: []});
  }

  async demon(n, isNoError = true) {
    while (isNoError) {
      console.log(`Start proccess`);
      try {
        const result = await this.bodyAsyncForDemon();
        this.logger.info(`Success, next recalculation in ${n / 1000 / 60 / 60} h`, { result: JSON.stringify(result) });
        await this.delay(n);
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


  async getCurrencies (): Promise<ICurrency[]> {
    const currencies: Array<ICurrency> = await Currency.find({}, null, { lean: true });
    return currencies;
  }

  async getEffectiveBalancesFromCollection ({ids = [], effectiveDate = null}): Promise<any[]> {

    const currencies = (await this.getCurrencies()).map(i => i.currency);

    const currQueriesFirst = currencies.reduce((acc, curr) => {
      acc[curr] = {
        '$cond' : {
          'if' : {
              '$eq' : [
                  '$_id.currency',
                  curr
              ]
          },
          'then' : '$totalEffectiveBalance',
          'else' : 0.0
          }
        };
      return acc;
    }, {});

    const currQueriesSecond = currencies.reduce((acc, curr) => {
      return {
        ...acc,
        [curr]: {'$sum' : '$' + curr},
      };
    }, {});

    const aggregationQueryAccountsBalanceistory = [
    {
      '$group' : {
        '_id' : {
            'type' : '$type',
            'currency' : '$currency'
        },
        'totalEffectiveBalance' : {
            '$sum' : '$effectiveBalance'
        },
        'type' : {
            '$first' : '$type'
        },
        'currency' : {
            '$first' : '$currency'
        }
      }
    },
    {
      '$lookup' : {
        'from' : 'dic_accountancy',
        'localField' : '_id.type',
        'foreignField' : 'code',
        'as' : 'description'
      }
    },
    {
      '$addFields' : {
        'description' : {
            '$arrayElemAt' : [
                '$description',
                0.0
            ]
        }
      }
    },
    {
      '$addFields' : {
        'type' : '$description.type',
        'name' : '$description.name'
      }
    },
    {
      '$project': {
        '_id' : 1,
        'type' : 1,
        'name' : 1,
        'currency': 1,
        'totalEffectiveBalance' : {
            '$cond' : {
                'if' : {
                    '$eq' : [
                        '$type',
                        'debit'
                    ]
                },
                'then' : {
                    '$multiply' : [
                        '$totalEffectiveBalance',
                        -1
                    ]
                },
                'else' : '$totalEffectiveBalance'
            }
         }
      }
    },
    {
      '$project': {
        '_id' : 1,
        'type' : 1,
        'name' : 1,
        ...currQueriesFirst
      }
    },
    {
      '$facet': {
        'accounts' : [
          {
              '$group' : {
                  '_id' : '$_id.type',
                  'name' : {
                      '$first' : '$name'
                  },
                  'type' : {
                      '$first' : '$type'
                  },
                  ...currQueriesSecond
              }
          },
          {
            '$sort': {
              'type' : -1,
              '_id' : 1
            }
          }
        ],
        'totals' : [
          {
              '$group' : {
                  '_id' : '$type',
                  ...currQueriesSecond
              }
          },
          {
              '$sort' : {
                  '_id' : -1
              }
          }
        ]

      }
    },
  ];
  const query = [
    {
      $match: {
        accountId: {'$in': [...ids.map(id => id + '')]},
        effectiveDate: moment(effectiveDate + 'T00:00:00.000Z').toDate()
      }
    },
    ...aggregationQueryAccountsBalanceistory
  ];

  const result = (await AccountsBalanceHistory.aggregate(query))
    .reduce((acc, i) => { acc = {...acc, ...i}; return acc; } , {});
  return result;

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
  public countEffectiveDates(startDate, stopDate) {
    let dateArray = new Array();
    let currentDate = startDate;
    while (currentDate <= stopDate) {
        dateArray = [...dateArray, this.formatDate(currentDate)];
        currentDate = new Date(currentDate).setDate(new Date(currentDate).getDate() + 1);
    }
    return dateArray;

  }


  async makeReport({ dates, ids }) {
    return this.fireRecalcEffectiveBalance({dates, ids});
  }

  async getEffectiveBalances (effectiveDate) {
    const brokerAccountsID = await this.getBrockerAccountsId();
    const result = await this.getEffectiveBalancesFromCollection({ids: brokerAccountsID, effectiveDate});
    return result;
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
          const {currency, accountId, effectiveDate, totalFromFiltered, totalToFiltered, effectiveBalance, type } = balance;
          return ({
            updateOne: {
              filter: { currency, accountId, effectiveDate },
              update: {
                $set: { updatedAt: timestamp, totalFromFiltered, totalToFiltered, effectiveBalance, type }
              },
              upsert: true,
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
      const output = balances.length ? await AccountsBalanceHistory.collection.bulkWrite(this.balancesUpdate(balances), options) : {};
      const { result= {} } = output;
      result['data'] = balances;
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
            {$match: { }},
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

  async getBrockerAccountsId(): Promise<any[]> {
    return ((await User.aggregate(
      [
        {
          $match: {
            type: 'broker',
          }
        },
        {
          $project: {
            accounts: 1,
            type: 1,
          }
        }
      ]
    )).map(_id => _id.accounts)).reduce((acc, val) => acc.concat(val), []);
  }


  async fireRecalcEffectiveBalance ({dates, ids }): Promise<any> {

    const effectiveDates = dates.map(date => new Date(date));
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
    const count = await AccountsBalanceHistory.find().countDocuments();

    let result;
    if (count === 0) {
      result = await this.inserMany(effectiveBalances);
    } else {
      result = await this.bulkWrite(effectiveBalances);
    }

    return result;
  }

}
