import { Schema, Model, model, Document, Types } from 'mongoose';

export interface IEffectiveBalance extends Document {
  status: string;
  type: string;
  currency: string;
  accountId: string;
  totalFromFiltered?: number;
  totalToFiltered?: number;
  effectiveBalance?: number;
  effectiveDate: Date;
  createdAt: Date;
}

export const AccountsBalanceHistorySchema: Schema = new Schema({
  status: String,
  type: String,
  currency: String,
  accountId: String,
  effectiveDate: Date,
  totalFromFiltered: {
    type: Number,
    default: 0
  },
  totalToFiltered: {
    type: Number,
    default: 0
  },
  effectiveBalance: {
    type: Number,
    default: 0
  },
  createdAt: Date
});

AccountsBalanceHistorySchema.pre<IEffectiveBalance>('save', function(next) {
  const now = new Date();

  if (!this.createdAt) {
    this.createdAt = now;
  }
  next();
});


AccountsBalanceHistorySchema.set('toJSON', {
  getters: false,
  versionKey: false,
  transform: (doc, ret) => {
    const now = new Date().toISOString();
    return ret;
  }
});

// tslint:disable-next-line:max-line-length
export const AccountsBalanceHistory: Model<IEffectiveBalance> = model<IEffectiveBalance>('AccountsBalanceHistory', AccountsBalanceHistorySchema);
