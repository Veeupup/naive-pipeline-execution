use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use pipeline_execution_toy::*;
use std::sync::Arc;

/**
 * Construt a simple pipeline:
 *      source1 -> Add('a', 'b')
 *
 *                                 ----> Accumulate('a + b', Sum)
 *      
 *      source2 -> Add('a', 'b')
 */
fn main() -> Result<()> {
    let mut pipeline = Pipeline::new(2);

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]));

    pipeline.add_source(Arc::new(MemorySource::new(
        vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
            ],
        )?],
        pipeline.graph.clone(),
    )));

    pipeline.add_source(Arc::new(MemorySource::new(
        vec![RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![100, 200, 300])),
                Arc::new(Int32Array::from(vec![400, 500, 600])),
            ],
        )?],
        pipeline.graph.clone(),
    )));

    pipeline.add_transform(|graph| {
        Arc::new(ArithmeticTransform::new("add", Operator::Add, 0, 1, graph))
    });

    pipeline.merge_processor(Accumulator::Sum, Some(0));

    let output = pipeline.execute()?;

    let expected_data = vec![RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("sum", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![2121]))],
    )?];

    pretty_assertions::assert_eq!(output, expected_data);

    Ok(())
}
