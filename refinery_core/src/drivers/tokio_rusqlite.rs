use crate::traits::r#async::{AsyncMigrate, AsyncQuery, AsyncTransaction};
use crate::Migration;
use tokio_rusqlite::{Connection as RqlConnection, Error as RqlError};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

async fn query_applied_migrations(
    transaction: &RqlConnection,
    query: &str,
) -> Result<Vec<Migration>, RqlError> {
    let mut stmt = transaction.prepare(query).await?;
    let mut rows = stmt.query([])?;
    let mut applied = Vec::new();
    while let Some(row) = rows.next()? {
        let version = row.get(0)?;
        let applied_on: String = row.get(2)?;
        // Safe to call unwrap, as we stored it in RFC3339 format on the database
        let applied_on = OffsetDateTime::parse(&applied_on, &Rfc3339).unwrap();

        let checksum: String = row.get(3)?;
        applied.push(Migration::applied(
            version,
            row.get(1)?,
            applied_on,
            checksum
                .parse::<u64>()
                .expect("checksum must be a valid u64"),
        ));
    }
    Ok(applied)
}

impl AsyncTransaction for RqlConnection {
    type Error = RqlError;

    async fn execute(&mut self, queries: &[&str]) -> Result<usize, Self::Error> {
        let transaction = self.transaction().await?;
        let mut count = 0;
        for query in queries.iter() {
            transaction.execute_batch(query).await?;
            count += 1;
        }
        transaction.commit().await?;
        Ok(count)
    }
}

impl AsyncQuery<Vec<Migration>> for RqlConnection {
    async fn query(&mut self, query: &str) -> Result<Vec<Migration>, <Self as AsyncTransaction>::Error> {
        let transaction = self.transaction().await?;
        let applied = query_applied_migrations(&transaction, query).await?;
        transaction.commit().await?;
        Ok(applied)
    }
}

impl AsyncMigrate for RqlConnection {}


mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("src/sql_migrations");
}


pub async fn run_migrations(&mut conn: RqlConnection) -> Result<()> {
    embedded::migrations::runner().run_async(&mut conn).await?;
}
