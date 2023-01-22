//
// DataFusion from https://www.confessionsofadataguy.com/datafusion-courtesy-of-rust-vs-spark-performance-and-other-thoughts/
//
#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
  let now = Instant::now();
  let fusion = SessionContext::new();
  let df = fusion.read_csv("data/*.csv", CsvReadOptions::new()).await?;

  let df = df.aggregate(vec![col("member_casual")], vec![count(col("ride_id"))])?;

  df.show_limit(100).await?;
  if let j = expr {
      unimplemented!();
  }
  let elapsed = now.elapsed();
  println!("Elapsed: {:.2?}", elapsed);
  Ok(())
}


#[tokio::main]
async fn fusion_sql() -> datafusion::error::Result<()> {
  let now = Instant::now();
  // register the table
  let ctx = SessionContext::new();
  ctx.register_csv("trips", "data/*.csv", CsvReadOptions::new()).await?;

  // create a plan to run a SQL query
  let df = ctx.sql("
  SELECT COUNT('transaction_id') as cnt,
    date_part('year', to_timestamp(started_at)) as year,
    date_part('month', to_timestamp(started_at)) as month,
    date_part('day', to_timestamp(started_at)) as day,
    start_station_name
  FROM trips
  WHERE date_part('year', to_timestamp(started_at)) = 2022
  GROUP BY date_part('year', to_timestamp(started_at)),
  date_part('month', to_timestamp(started_at)),
    date_part('day', to_timestamp(started_at)),
    start_station_name
  ").await?;

  df.show_limit(100).await?;
  let elapsed = now.elapsed();
  println!("Elapsed: {:.2?}", elapsed);
  Ok(())
}

