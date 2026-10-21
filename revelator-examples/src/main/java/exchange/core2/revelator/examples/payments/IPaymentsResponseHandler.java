package exchange.core2.revelator.examples.payments;

public interface IPaymentsResponseHandler {

    void commandResult(long timestamp,
                       long correlationId,
                       int resultCode,
                       IRequestAccessor accessor);


    void balanceUpdateEvent(long account, long diff, long newBalance);


    interface IRequestAccessor {

        byte getCommandType();
    }

    // Todo extract framework part

    interface ITestControlCmdAccessor extends IRequestAccessor {

        int getMsgSize();

        byte getMsgType();

        long getData(int offset);

        long[] getData();
    }

    interface ITransferAccessor extends IRequestAccessor {

        long getAccountFrom();

        long getAccountTo();

        long getAmount();

        int getCurrency();
    }

    interface IAdjustBalanceAccessor extends IRequestAccessor {

        long getAccount();

        long getAmount();
    }

    interface IOpenAccountAccessor extends IRequestAccessor {

        long getAccount();

    }

    interface ICloseAccountAccessor extends IRequestAccessor {

        long getAccount();

    }


    interface IUnsupportedAccessor extends IPaymentsResponseHandler.IRequestAccessor {

    }
}
