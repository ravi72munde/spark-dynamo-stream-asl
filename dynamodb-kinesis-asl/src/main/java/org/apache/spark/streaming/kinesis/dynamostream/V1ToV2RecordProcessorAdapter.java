package org.apache.spark.streaming.kinesis.dynamostream;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;

class V1ToV2RecordProcessorAdapter implements IRecordProcessor {

    private com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor recordProcessor;

    V1ToV2RecordProcessorAdapter(
            com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor recordProcessor) {
        this.recordProcessor = recordProcessor;
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        recordProcessor.initialize(initializationInput.getShardId());
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        recordProcessor.processRecords(processRecordsInput.getRecords(), processRecordsInput.getCheckpointer());

    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        recordProcessor.shutdown(shutdownInput.getCheckpointer(), shutdownInput.getShutdownReason());
    }

}