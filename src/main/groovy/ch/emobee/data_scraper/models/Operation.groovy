package ch.emobee.data_scraper.models

import ch.emobee.data_scraper.Calculator

import java.util.logging.Logger


class Operation {

    public final static int OPERATION_TYPE_ALL_CALC = 0
    public final static int OPERATION_TYPE_TOTAL_COUNTED_DELAYS = 1

    private final logger = Logger.getLogger(Operation.toString())
    private int calcType
    private String result

    void start(calcType) {
        this.calcType = calcType
        switch (calcType) {
            case OPERATION_TYPE_ALL_CALC:
                setResult(new Calculator().runAllCalculations())
                break
            default:
                logger.warning("Invalid calcType entered.")
        }
    }

    String getResult() {
        return result
    }

    void setResult(String result) {
        this.result = result
    }
}
