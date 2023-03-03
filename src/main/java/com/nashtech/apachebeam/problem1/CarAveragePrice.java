package com.nashtech.apachebeam.problem1;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This [[ApacheBeamTextIO]] represents a beam pipeline that reads car data(csv file)
 * and compute the average price of different Cars and result is writing to
 * another csv file with header [car,avg_price].
 */

public class CarAveragePrice {

    //  CSV_HEADER initializes as private for car & price
    private static final String CSV_HEADER = "car,price";
    private static final Logger LOGGER = LoggerFactory.getLogger(CarAveragePrice.class);

    // main method called
    public static void main(String[] args) {

        final AveragePriceProcessingOptions averagePriceProcessingOptions = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(AveragePriceProcessingOptions.class);

        Pipeline pipeline = Pipeline.create(averagePriceProcessingOptions);

        // applied pipeline to read the required file
        pipeline.apply("Read-Lines", TextIO.read()
                        .from(averagePriceProcessingOptions.getInputFile()))
                .apply("Filter-Header", Filter.by((String line) ->
                        !line.isEmpty() && !line.contains(CSV_HEADER)))
                .apply("Map", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                        .via((String line) -> {
                            String[] tokens = line.split(",");
                            return KV.of(tokens[0], Double.parseDouble(tokens[1]));
                        }))
                .apply("AverageAggregation", Mean.perKey())
                .apply("Format-result", MapElements
                        .into(TypeDescriptors.strings())
                        .via(carCount -> carCount.getKey() + "," + carCount.getValue()))

                // applied pipeline to write or modify the required file
                .apply("WriteResult", TextIO.write()
                        .to(averagePriceProcessingOptions.getOutputFile())
                        .withoutSharding()
                        .withSuffix(".csv")
                        .withHeader("car,Avg_price"));

        pipeline.run();

        LOGGER.info("pipeline executed successfully");   // result outcomes message
    }
    public interface AveragePriceProcessingOptions extends PipelineOptions {
        Logger LOGGER = LoggerFactory.getLogger(AveragePriceProcessingOptions.class);
        @Description("Path of the file to read from")
        @Default.String("src/main/resources/Car_file.csv")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write")
        @Default.String("src/main/resources/sink/Car_Avg_price.csv")
        String getOutputFile();

        void setOutputFile(String value);
    }
}
