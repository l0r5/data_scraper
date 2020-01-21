package ch.emobee.data_scraper.models

import ch.emobee.data_scraper.Calculator

import java.util.logging.Logger


class Operation {

    public final static int OPERATION_TYPE_ALL_CALC = 0
    public final static int OPERATION_TYPE_TOTAL_COUNTED_DELAYS = 1

    private final logger = Logger.getLogger(Operation.toString())
    private int _operationType
    private String _result

    void runOperation(operationType) {
        _operationType = operationType
        _result = new Calculator().calc(_operationType)
    }

    String getResult() {
        return _result
    }

}
