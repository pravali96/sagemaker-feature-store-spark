package software.amazon.sagemaker.featurestore.sparksdk

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

object SparkRowEncoderAdaptor {
  def encoderFor(schema: StructType): ExpressionEncoder[Row] = {
    // For Spark 3.5+, RowEncoder.encoderFor(schema) is the API and returns an AgnosticEncoder or ExpressionEncoder depending on usage.
    // Fortunately casting to ExpressionEncoder[Row] or the implicit conversion is typically compatible, 
    // but the underlying method is `.encoderFor()`. To be safe, we just use RowEncoder.encoderFor
    RowEncoder.encoderFor(schema).asInstanceOf[ExpressionEncoder[Row]]
  }
}
