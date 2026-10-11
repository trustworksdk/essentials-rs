use std::fmt;

use crate::error::{DurableQueuesError, Result};

/// PostgreSQL's default identifier length limit (`NAMEDATALEN - 1`).
pub const MAX_IDENTIFIER_LENGTH: usize = 63;

/// A validated, lower-cased queue table name - the only value this crate splices into SQL.
///
/// Mirrors `PostgresqlUtil.checkIsValidTableOrColumnName` (CONTRACT.md §9) with one deliberate restriction:
/// schema-qualified names are rejected, because the index names are derived from the table name
/// (`idx_{table}_…`) and a qualified name produces invalid DDL - in Java as well.
///
/// Validation is a defence layer, not sanitisation: table names must come from configuration, never from user input.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableName(String);

impl TableName {
    /// Java's `PostgresqlDurableQueues.DEFAULT_DURABLE_QUEUES_TABLE_NAME`.
    pub const DEFAULT: &'static str = "durable_queues";

    /// Lower-cases `name` (as Java does) and validates it.
    pub fn new(name: impl AsRef<str>) -> Result<Self> {
        let name = name.as_ref().to_ascii_lowercase();
        if let Some(reason) = Self::rejection_reason(&name) {
            return Err(DurableQueuesError::InvalidTableName { name, reason });
        }
        Ok(Self(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn rejection_reason(name: &str) -> Option<&'static str> {
        if name.trim().is_empty() {
            return Some("must not be blank");
        }
        if name.contains('.') {
            return Some("schema-qualified names are not supported");
        }
        if name.len() > MAX_IDENTIFIER_LENGTH {
            return Some("longer than 63 characters");
        }
        let mut chars = name.chars();
        let starts_ok = chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
        if !starts_ok || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Some(
                "must start with a letter or underscore, followed by letters, digits or underscores",
            );
        }
        if RESERVED_NAMES
            .iter()
            .any(|reserved| reserved.eq_ignore_ascii_case(name))
        {
            return Some("is a reserved SQL keyword or PostgreSQL type name");
        }
        None
    }
}

impl Default for TableName {
    fn default() -> Self {
        Self(Self::DEFAULT.to_owned())
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// `PostgresqlUtil.RESERVED_NAMES` from Essentials 0.60.0, verbatim.
const RESERVED_NAMES: &[&str] = &[
    "BIGINT",
    "INT8",
    "BIGSERIAL",
    "SERIAL8",
    "BIT",
    "VARBIT",
    "BOOLEAN",
    "BOOL",
    "BOX",
    "BYTEA",
    "CHARACTER",
    "CHAR",
    "VARYING",
    "VARCHAR",
    "CIDR",
    "CIRCLE",
    "DATE",
    "DOUBLE",
    "PRECISION",
    "FLOAT8",
    "INET",
    "INTEGER",
    "INT",
    "INT4",
    "INTERVAL",
    "JSON",
    "JSONB",
    "LINE",
    "LSEG",
    "MACADDR",
    "MACADDR8",
    "MONEY",
    "NUMERIC",
    "DECIMAL",
    "PATH",
    "PG_LSN",
    "POINT",
    "POLYGON",
    "REAL",
    "FLOAT4",
    "SMALLINT",
    "INT2",
    "SMALLSERIAL",
    "SERIAL2",
    "SERIAL",
    "SERIAL4",
    "TEXT",
    "TIME",
    "TIMETZ",
    "TIMESTAMPTZ",
    "TSQUERY",
    "TSVECTOR",
    "TXID_SNAPSHOT",
    "UUID",
    "XML",
    "ALL",
    "ANALYSE",
    "ANALYZE",
    "AND",
    "ANY",
    "ARRAY",
    "AS",
    "ASC",
    "ASYMMETRIC",
    "AUTHORIZATION",
    "BINARY",
    "BOTH",
    "CASE",
    "CAST",
    "CHECK",
    "COLLATE",
    "COLLATION",
    "COLUMN",
    "CONSTRAINT",
    "CREATE",
    "CROSS",
    "CURRENT_CATALOG",
    "CURRENT_DATE",
    "CURRENT_ROLE",
    "CURRENT_SCHEMA",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "DEFAULT",
    "DEFERRABLE",
    "DESC",
    "DISTINCT",
    "DO",
    "ELSE",
    "END",
    "EXCEPT",
    "FALSE",
    "FETCH",
    "FOR",
    "FOREIGN",
    "FREEZE",
    "FROM",
    "FULL",
    "GRANT",
    "GROUP",
    "HAVING",
    "ILIKE",
    "IN",
    "INITIALLY",
    "INNER",
    "INTERSECT",
    "INTO",
    "IS",
    "ISNULL",
    "JOIN",
    "LEADING",
    "LEFT",
    "LIKE",
    "LIMIT",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "NATURAL",
    "NOT",
    "NOTNULL",
    "NULL",
    "OFFSET",
    "ON",
    "ONLY",
    "OR",
    "ORDER",
    "OUTER",
    "OVERLAPS",
    "PLACING",
    "PRIMARY",
    "REFERENCES",
    "RETURNING",
    "RIGHT",
    "SELECT",
    "SESSION_USER",
    "SIMILAR",
    "SOME",
    "SYMMETRIC",
    "TABLE",
    "THEN",
    "TO",
    "TRAILING",
    "TRUE",
    "UNION",
    "UNIQUE",
    "USER",
    "USING",
    "VARIADIC",
    "VERBOSE",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WITH",
    "DROP",
    "EXISTS",
    "EXPLAIN",
    "CLOB",
    "BLOB",
    "NBLOB",
    "NCHAR",
    "SAVEPOINT",
    "TIMESTAMPZ",
    "VACUUM",
    "VIEW",
    "ABS",
    "ALLOCATE",
    "ALTER",
    "ARE",
    "ASENSITIVE",
    "AT",
    "ATOMIC",
    "BEGIN",
    "BETWEEN",
    "CALL",
    "CALLED",
    "CEIL",
    "CEILING",
    "CLOSE",
    "COALESCE",
    "COMMIT",
    "CONNECT",
    "CONNECTION",
    "CONVERT",
    "CORR",
    "CORRESPONDING",
    "COUNT",
    "COVAR_POP",
    "COVAR_SAMP",
    "CUBE",
    "CUME_DIST",
    "CURRENT",
    "CURRENT_DEFAULT_TRANSFORM_GROUP",
    "CURRENT_PATH",
    "CURRENT_ROW",
    "CURRENT_TRANSFORM_GROUP_FOR_TYPE",
    "CURSOR",
    "CYCLE",
    "DAY",
    "DEALLOCATE",
    "DECLARE",
    "DELETE",
    "DENSE_RANK",
    "DEREF",
    "DESCRIBE",
    "DETERMINISTIC",
    "DISCONNECT",
    "END-EXEC",
    "ESCAPE",
    "EVERY",
    "EXEC",
    "EXCEPTION",
    "EXECUTE",
    "EXIT",
    "EXP",
    "EXTERNAL",
    "EXTRACT",
    "FILTER",
    "FIRST",
    "FLOOR",
    "FOUND",
    "FUNCTION",
    "FUSION",
    "GET",
    "GLOBAL",
    "GROUPING",
    "HOLD",
    "HOUR",
    "IDENTITY",
    "IMMEDIATE",
    "INDICATOR",
    "INOUT",
    "INPUT",
    "INSENSITIVE",
    "INSERT",
    "KEY",
    "LAG",
    "LANGUAGE",
    "LARGE",
    "LAST",
    "LATERAL",
    "LEAD",
    "LEVEL",
    "LOCAL",
    "MATCH",
    "MAX",
    "MEMBER",
    "MERGE",
    "METHOD",
    "MIN",
    "MINUTE",
    "MOD",
    "MODIFIES",
    "MODULE",
    "MONTH",
    "MULTISET",
    "NCLOB",
    "NEW",
    "NO",
    "NONE",
    "NORMALIZE",
    "NULLIF",
    "OBJECT",
    "OCCURRENCES_REGEX",
    "OCTETS",
    "OF",
    "OLD",
    "OPEN",
    "OPERATION",
    "OPTIONS",
    "ORDINALITY",
    "OUT",
    "OUTPUT",
    "OVER",
    "OVERLAY",
    "PAD",
    "PARAMETER",
    "PARTITION",
    "PERCENT",
    "PERCENT_RANK",
    "PERCENTILE_CONT",
    "PERCENTILE_DISC",
    "POSITION",
    "POWER",
    "PRECEDING",
    "PREPARE",
    "PROCEDURE",
    "RANGE",
    "RANK",
    "READS",
    "RECURSIVE",
    "REF",
    "REFERENCING",
    "REGR_AVGX",
    "REGR_AVGY",
    "REGR_COUNT",
    "REGR_INTERCEPT",
    "REGR_R2",
    "REGR_SLOPE",
    "REGR_SXX",
    "REGR_SXY",
    "REGR_SYY",
    "RELATIVE",
    "RELEASE",
    "REPEAT",
    "RESIGNAL",
    "RESTRICT",
    "RESULT",
    "RETURN",
    "RETURNS",
    "REVOKE",
    "ROLE",
    "ROLLUP",
    "ROW",
    "ROW_NUMBER",
    "ROWS",
    "SCOPE",
    "SCROLL",
    "SEARCH",
    "SECOND",
    "SECTION",
    "SENSITIVE",
    "SET",
    "SIGNAL",
    "SPECIFIC",
    "SPECIFICTYPE",
    "SQL",
    "SQLEXCEPTION",
    "SQLSTATE",
    "SQLWARNING",
    "SQRT",
    "STACKED",
    "START",
    "STATIC",
    "STDDEV_POP",
    "STDDEV_SAMP",
    "SUBSTRING",
    "SUM",
    "SYSTEM",
    "SYSTEM_USER",
    "TABLESAMPLE",
    "TIMEZONE_HOUR",
    "TIMEZONE_MINUTE",
    "TRANSLATE",
    "TRANSLATE_REGEX",
    "TRANSLATION",
    "TREAT",
    "TRIGGER",
    "TRIM",
    "UESCAPE",
    "UNBOUNDED",
    "UNKNOWN",
    "UNNEST",
    "UNTIL",
    "UPDATE",
    "VALUE",
    "VALUES",
    "VAR_POP",
    "VAR_SAMP",
    "VARBINARY",
    "WIDTH_BUCKET",
    "WITHIN",
    "WITHOUT",
    "WORK",
    "WRITE",
    "XMLATTRIBUTES",
    "XMLBINARY",
    "XMLCAST",
    "XMLCOMMENT",
    "XMLCONCAT",
    "XMLELEMENT",
    "XMLEXISTS",
    "XMLFOREST",
    "XMLITERATE",
    "XMLNAMESPACES",
    "XMLPARSE",
    "XMLPI",
    "XMLQUERY",
    "XMLROOT",
    "XMLSCHEMA",
    "XMLSERIALIZE",
    "XMLTABLE",
    "YEAR",
    "ZONE",
];

#[cfg(test)]
mod tests {
    use super::*;

    fn rejected(name: &str) -> bool {
        matches!(
            TableName::new(name),
            Err(DurableQueuesError::InvalidTableName { .. })
        )
    }

    #[test]
    fn default_is_durable_queues() {
        assert_eq!(TableName::default().as_str(), "durable_queues");
    }

    #[test]
    fn accepts_and_lower_cases_valid_identifiers() {
        assert_eq!(
            TableName::new("Orders_Queue").unwrap().as_str(),
            "orders_queue"
        );
        assert_eq!(TableName::new("_q1").unwrap().as_str(), "_q1");
        assert!(TableName::new("a".repeat(MAX_IDENTIFIER_LENGTH)).is_ok());
    }

    #[test]
    fn rejects_malformed_identifiers() {
        for name in [
            "",
            "   ",
            "1queue",
            "queue-name",
            "queue name",
            "kø",
            "a".repeat(64).as_str(),
        ] {
            assert!(rejected(name), "expected '{name}' to be rejected");
        }
    }

    #[test]
    fn rejects_injection_attempts() {
        for name in [
            "q; DROP TABLE users",
            "q--",
            "q'",
            "q\"",
            "q/*",
            "durable_queues WHERE 1=1",
        ] {
            assert!(rejected(name), "expected '{name}' to be rejected");
        }
    }

    #[test]
    fn rejects_qualified_names() {
        assert!(rejected("public.durable_queues"));
    }

    #[test]
    fn rejects_reserved_names_case_insensitively() {
        for name in ["select", "TABLE", "Jsonb", "user", "order"] {
            assert!(rejected(name), "expected '{name}' to be rejected");
        }
    }
}
