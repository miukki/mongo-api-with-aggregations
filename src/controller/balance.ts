import { Container, Service } from 'typedi';
import { NextFunction, Request, Response } from 'express';

import { environment } from '../environments/environment';
import BigNumber from 'bignumber.js';
import { deepConvertNumberDecimal } from '../models/utils';

import { AccountsBalanceHistory, IEffectiveBalance } from '../models/balances';
import { Account, AccountType } from '../models/accounts';

import { LoggerService } from '../services/logger.service';
import { Balance } from 'ccxt';

import { ClientSession } from 'mongodb';

const {
  performance,
} = require('perf_hooks');

@Service()
export class BalanceController {
  private logger: LoggerService = Container.get(LoggerService);

  constructor() {
  }

  async inserToAccountsBalanceHistory  (
    balances: Array<IEffectiveBalance>,
    session?: ClientSession,
  ): Promise<IEffectiveBalance[]> {
    try {
      let output;
      const count = await AccountsBalanceHistory.find().countDocuments();
      const options = { ordered: false, session };
      if (count === 0) {
        output = await AccountsBalanceHistory.insertMany(
          balances,
          options
        ) as any;
      } else {
        output = await AccountsBalanceHistory.updateMany(
          balances,
          options
        ) as any;
      }

      console.log('output', output);
      return output;
    } catch (error) {
      this.logger.error({ error });
    }
  }
  public getEffectiveBalance = async (req: Request, res: Response, next: NextFunction) => {
    const { date } = req.query;
    let effectiveDate = new Date(date).toISOString();

    const before = performance.now();
    const effectiveBalances: Array<IEffectiveBalance> = await Account.aggregate([
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
    const after = performance.now();
    const mills = (after - before);

    // mills ~5025.575729995966
    console.log('effectiveBalances', effectiveBalances, 'mills', mills);

    // do push createdAt for each item collection
    // const now = new Date();
    //  effectiveBalances.reduce((acc, item, index) => [
    //   ...acc,
    //   {
    //     item,
    //     createdAt: now
    //   }
    // ], []);

    console.log('this.inserToAccountsBalanceHistory', this.inserToAccountsBalanceHistory);
    const result = await this.inserToAccountsBalanceHistory(effectiveBalances);

    res.send(result).status(200);
  }

}
