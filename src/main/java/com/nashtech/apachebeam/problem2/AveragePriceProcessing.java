package com.nashtech.apachebeam.problem2;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This [[ApacheBeamTextIO]] represents a beam pipeline that reads google stock data(csv file)
 * and compute the average closing price of every month of year 2020.And result is writing to
 * another csv file with header [car,avg_price].
 */

public class AveragePriceProcessing {

    private static final Logger LOGGER = LoggerFactory.getLogger(AveragePriceProcessing.class);

    private static final String CSV_HEADER = "Date,Open,High,Low,Close,Adj Close,Volume";

    public static void main(String[] args) {

//      Start by defining the options for the pipeline.
        final AveragePriceProcessingOptions averagePriceProcessingOptions = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(AveragePriceProcessingOptions.class);

//      Then create the pipeline.
        Pipeline pipeline = Pipeline.create(averagePriceProcessingOptions);

        /** Create and apply the PCollection 'lines' by applying a 'Read' transform.
         *  Reading Input data
         * */
        pipeline.apply("Read-Lines", TextIO.read()
                        .from(averagePriceProcessingOptions.getInputFile()))
                .apply("Filter-Header", Filter.by((String line) ->
                        !line.isEmpty() && !line.contains(CSV_HEADER)))

//                Apply a MapElements with an anonymous lambda function.
                .apply("Map", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                        .via((String line) -> {
                            String[] tokens = line.split(",");
                            return KV.of(line.split("-")[1], Double.parseDouble(tokens[4]));
                        }))
                .apply("AverageAggregation", Mean.perKey())
                .apply("Format-result", MapElements
                        .into(TypeDescriptors.strings())
                        .via(carCount -> carCount.getKey() + "," + carCount.getValue()))

//               Writing output data to car_avg_price.csv file
                .apply("WriteResult", TextIO.write()
                        .to(averagePriceProcessingOptions.getOutputFile())
                        .withoutSharding()
                        .withSuffix(".csv")
                        .withHeader("month,avg_price"));

//      Run the pipeline
        pipeline.run();
        LOGGER.info("pipeline executed successfully");
    }

    //     Creating our own custom options
    public interface AveragePriceProcessingOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("src/main/resources/sink2/google_stock_2020.csv")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write")
        @Default.String("src/main/resources/sink2/google_avg_price.csv")
        String getOutputFile();

        void setOutputFile(String value);
    }
}
