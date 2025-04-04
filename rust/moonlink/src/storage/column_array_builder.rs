use crate::row::RowValue;
use arrow::array::builder::{ArrayBuilder, BooleanBuilder, PrimitiveBuilder, StringBuilder};
use arrow::array::types::{Float32Type, Float64Type, Int32Type, Int64Type};
use arrow::array::{ArrayRef, FixedSizeBinaryBuilder};
use arrow::datatypes::DataType;

/// A column array builder that can handle different types
pub struct ColumnArrayBuilder {
    builder: Box<dyn ArrayBuilder>,
    data_type: DataType,
}

impl ColumnArrayBuilder {
    /// Create a new column array builder for a specific data type
    pub fn new(data_type: DataType, capacity: usize) -> Self {
        let builder: Box<dyn ArrayBuilder> = match &data_type {
            DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
            DataType::Int16 | DataType::Int32 | DataType::Date32 => {
                Box::new(PrimitiveBuilder::<Int32Type>::with_capacity(capacity))
            }
            DataType::Timestamp(_, _) | DataType::Int64 | DataType::Time64(_) => {
                Box::new(PrimitiveBuilder::<Int64Type>::with_capacity(capacity))
            }
            DataType::Float32 => Box::new(PrimitiveBuilder::<Float32Type>::with_capacity(capacity)),
            DataType::Float64 => Box::new(PrimitiveBuilder::<Float64Type>::with_capacity(capacity)),
            DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, capacity * 10)),
            DataType::FixedSizeBinary(_size) => {
                assert_eq!(*_size, 16);
                Box::new(FixedSizeBinaryBuilder::with_capacity(capacity, 16))
            }
            _ => panic!("data type: {:?}", data_type),
        };

        Self { builder, data_type }
    }

    /// Append a value to this builder
    pub fn append_value(&mut self, value: &RowValue) -> Result<(), String> {
        match value {
            RowValue::Int32(v) => {
                self.builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveBuilder<Int32Type>>()
                    .unwrap()
                    .append_value(*v);
            }
            RowValue::Int64(v) => {
                self.builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveBuilder<Int64Type>>()
                    .unwrap()
                    .append_value(*v);
            }
            RowValue::Float32(v) => {
                self.builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveBuilder<Float32Type>>()
                    .unwrap()
                    .append_value(*v);
            }
            RowValue::Float64(v) => {
                self.builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveBuilder<Float64Type>>()
                    .unwrap()
                    .append_value(*v);
            }
            RowValue::Decimal(_v) => {
                todo!("implementdecimal: {:?}", _v);
            }
            RowValue::Bool(v) => {
                self.builder
                    .as_any_mut()
                    .downcast_mut::<BooleanBuilder>()
                    .unwrap()
                    .append_value(*v);
            }
            RowValue::ByteArray(v) => {
                let s = unsafe { std::str::from_utf8_unchecked(&v) };
                self.builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .unwrap()
                    .append_value(s);
            }
            RowValue::FixedLenByteArray(v) => {
                let _ret = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<FixedSizeBinaryBuilder>()
                    .unwrap()
                    .append_value(v);
                assert!(_ret.is_ok());
            }
            RowValue::Null => {
                self.append_null();
            }
        }
        Ok(())
    }

    /// Append a null value to this builder
    pub fn append_null(&mut self) {
        match self.data_type {
            DataType::Boolean => self
                .builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap()
                .append_null(),
            DataType::Int16 | DataType::Int32 | DataType::Date32 => self
                .builder
                .as_any_mut()
                .downcast_mut::<PrimitiveBuilder<Int32Type>>()
                .unwrap()
                .append_null(),
            DataType::Int64 | DataType::Timestamp(_, _) => self
                .builder
                .as_any_mut()
                .downcast_mut::<PrimitiveBuilder<Int64Type>>()
                .unwrap()
                .append_null(),
            DataType::Float32 => self
                .builder
                .as_any_mut()
                .downcast_mut::<PrimitiveBuilder<Float32Type>>()
                .unwrap()
                .append_null(),
            DataType::Float64 => self
                .builder
                .as_any_mut()
                .downcast_mut::<PrimitiveBuilder<Float64Type>>()
                .unwrap()
                .append_null(),
            DataType::Utf8 => self
                .builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap()
                .append_null(),
            DataType::FixedSizeBinary(_) => self
                .builder
                .as_any_mut()
                .downcast_mut::<FixedSizeBinaryBuilder>()
                .unwrap()
                .append_null(),
            _ => todo!("data type: {:?}", self.data_type),
        }
    }

    /// Finish building and return the array
    pub fn finish(&mut self) -> ArrayRef {
        self.builder.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int32Array,
        Int64Array, StringArray,
    };
    use arrow::datatypes::DataType;

    #[test]
    fn test_column_array_builder() {
        // Test Int32 type
        let mut builder = ColumnArrayBuilder::new(DataType::Int32, 2);
        builder.append_value(&RowValue::Int32(1)).unwrap();
        builder.append_value(&RowValue::Int32(2)).unwrap();
        let array = builder.finish();
        assert_eq!(array.len(), 2);
        let int32_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_array.value(0), 1);
        assert_eq!(int32_array.value(1), 2);

        // Test Int64 type
        let mut builder = ColumnArrayBuilder::new(DataType::Int64, 2);
        builder.append_value(&RowValue::Int64(100)).unwrap();
        builder.append_value(&RowValue::Int64(200)).unwrap();
        let array = builder.finish();
        assert_eq!(array.len(), 2);
        let int64_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_array.value(0), 100);
        assert_eq!(int64_array.value(1), 200);

        // Test Float32 type
        let mut builder = ColumnArrayBuilder::new(DataType::Float32, 2);
        builder.append_value(&RowValue::Float32(3.14)).unwrap();
        builder.append_value(&RowValue::Float32(2.71)).unwrap();
        let array = builder.finish();
        assert_eq!(array.len(), 2);
        let float32_array = array.as_any().downcast_ref::<Float32Array>().unwrap();
        assert!((float32_array.value(0) - 3.14).abs() < 0.0001);
        assert!((float32_array.value(1) - 2.71).abs() < 0.0001);

        // Test Float64 type
        let mut builder = ColumnArrayBuilder::new(DataType::Float64, 2);
        builder.append_value(&RowValue::Float64(3.14159)).unwrap();
        builder.append_value(&RowValue::Float64(2.71828)).unwrap();
        let array = builder.finish();
        assert_eq!(array.len(), 2);
        let float64_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((float64_array.value(0) - 3.14159).abs() < 0.00001);
        assert!((float64_array.value(1) - 2.71828).abs() < 0.00001);

        // Test Boolean type
        let mut builder = ColumnArrayBuilder::new(DataType::Boolean, 2);
        builder.append_value(&RowValue::Bool(true)).unwrap();
        builder.append_value(&RowValue::Bool(false)).unwrap();
        let array = builder.finish();
        assert_eq!(array.len(), 2);
        let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(bool_array.value(0), true);
        assert_eq!(bool_array.value(1), false);

        // Test Utf8 (ByteArray) type
        let mut builder = ColumnArrayBuilder::new(DataType::Utf8, 2);
        builder
            .append_value(&RowValue::ByteArray("hello".as_bytes().to_vec()))
            .unwrap();
        builder
            .append_value(&RowValue::ByteArray("world".as_bytes().to_vec()))
            .unwrap();
        let array = builder.finish();
        assert_eq!(array.len(), 2);
        let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_array.value(0), "hello");
        assert_eq!(string_array.value(1), "world");

        // Test FixedSizeBinary type
        let mut builder = ColumnArrayBuilder::new(DataType::FixedSizeBinary(16), 2);
        let bytes1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let bytes2 = [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1];
        builder
            .append_value(&RowValue::FixedLenByteArray(bytes1))
            .unwrap();
        builder
            .append_value(&RowValue::FixedLenByteArray(bytes2))
            .unwrap();
        let array = builder.finish();
        assert_eq!(array.len(), 2);
        let binary_array = array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(binary_array.value(0), bytes1);
        assert_eq!(binary_array.value(1), bytes2);

        // Test null values
        let mut builder = ColumnArrayBuilder::new(DataType::Int32, 3);
        builder.append_value(&RowValue::Int32(1)).unwrap();
        builder.append_null();
        builder.append_value(&RowValue::Int32(3)).unwrap();
        let array = builder.finish();
        assert_eq!(array.len(), 3);
        let int32_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_array.value(0), 1);
        assert!(int32_array.is_null(1));
        assert_eq!(int32_array.value(2), 3);

        // Test using null values directly from RowValue::Null
        let mut builder = ColumnArrayBuilder::new(DataType::Int32, 3);
        builder.append_value(&RowValue::Int32(1)).unwrap();
        builder.append_value(&RowValue::Null).unwrap();
        builder.append_value(&RowValue::Int32(3)).unwrap();
        let array = builder.finish();
        assert_eq!(array.len(), 3);
        let int32_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_array.value(0), 1);
        assert!(int32_array.is_null(1));
        assert_eq!(int32_array.value(2), 3);
    }
}
