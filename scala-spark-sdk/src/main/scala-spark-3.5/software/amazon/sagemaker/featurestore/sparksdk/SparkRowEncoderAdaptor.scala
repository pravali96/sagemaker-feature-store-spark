package software.amazon.sagemaker.featurestore.sparksdk

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.StructType

object SparkRowEncoderAdaptor extends SparkRowEncoderAdaptorLike {
  override def encoderFor(schema: StructType): ExpressionEncoder[Row] = {
    // For Spark 3.5+, RowEncoder.encoderFor(schema) is the API and returns an AgnosticEncoder or ExpressionEncoder depending on usage.
    // Fortunately casting to ExpressionEncoder[Row] or the implicit conversion is typically compatible, 
    // but the underlying method is `.encoderFor()`. To be safe, we just use RowEncoder.encoderFor
    ExpressionEncoder(RowEncoder.encoderFor(schema))
  }
}