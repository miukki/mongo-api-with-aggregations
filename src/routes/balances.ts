import { Router } from 'express';
import { Container } from 'typedi';

import { BalanceController } from '../controllers/balance';
import { UserType } from '../models/users';
import { AuthController } from '../controllers';


const balanceController: BalanceController = Container.get(BalanceController);
const authController: AuthController = Container.get(AuthController);

const router = Router({ mergeParams: true }).use(authController.checkRole(UserType.Manager));

router.get('/balances/effective', balanceController.getEffectiveBalance);

export { router as balanceRouter };
