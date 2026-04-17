"""
PGA Natural Language Query Engine
Converts plain English questions to SQL and explains results using Claude API.
"""

import anthropic
import psycopg2
import psycopg2.extras
import json
import os
from schema import NL_TO_SQL_SYSTEM, EXPLAIN_RESULTS_SYSTEM

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))


db_config = {
    "host":     os.getenv("NEON_HOST"),
    "port":     int(os.getenv("NEON_PORT", 5432)),
    "database": os.getenv("NEON_DB"),
    "user":     os.getenv("NEON_USER"),
    "password": os.getenv("NEON_PASSWORD"),
    "sslmode":  "require"
}

# ─── Step 1: Natural Language → SQL ─────────────────────────────────────────

def generate_sql(question: str) -> str:
    """Convert a natural language question to a PostgreSQL query."""
    response = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=1000,
        system=NL_TO_SQL_SYSTEM,
        messages=[
            {"role": "user", "content": question}
        ]
    )
    return response.content[0].text.strip()


# ─── Step 2: Execute SQL ─────────────────────────────────────────────────────

def execute_query(sql: str) -> tuple:
    """Run a SQL query and return rows as list of dicts plus column names."""
    conn = psycopg2.connect(**db_config)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            return [dict(row) for row in rows], columns
    finally:
        conn.close()


# ─── Step 3: Results → Plain English Explanation ─────────────────────────────

def explain_results(question: str, sql: str, rows: list[dict], columns: list[str]) -> str:
    """Generate a plain English explanation of query results."""
    # Limit to first 20 rows for context window efficiency
    sample = rows[:20]
    results_text = json.dumps(sample, indent=2, default=str)

    prompt = f"""
Original question: {question}

SQL query used: {sql}

Results ({len(rows)} total rows):
{results_text}

Columns returned: {', '.join(columns)}
"""

    response = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=500,
        system=EXPLAIN_RESULTS_SYSTEM,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )
    return response.content[0].text.strip()


# ─── Main Query Pipeline ─────────────────────────────────────────────────────

def run_nl_query(question: str) -> dict:
    """
    Full pipeline: NL question → SQL → execute → explain.
    
    Returns dict with:
    - question: original question
    - sql: generated SQL
    - rows: query results
    - columns: column names
    - explanation: plain English summary
    - error: error message if something failed
    """
    result = {
        "question": question,
        "sql": None,
        "rows": [],
        "columns": [],
        "explanation": None,
        "error": None
    }

    # Step 1: Generate SQL
    try:
        sql = generate_sql(question)
        if sql.startswith("ERROR:"):
            result["error"] = sql
            return result
        result["sql"] = sql
    except Exception as e:
        result["error"] = f"Failed to generate SQL: {str(e)}"
        return result

    # Step 2: Execute query
    try:
        rows, columns = execute_query(sql)
        result["rows"] = rows
        result["columns"] = columns
    except Exception as e:
        result["error"] = f"Query execution failed: {str(e)}\n\nGenerated SQL:\n{sql}"
        return result

    # Step 3: Explain results
    try:
        if rows:
            explanation = explain_results(question, sql, rows, columns)
        else:
            explanation = "No results found for this query. The filters may be too narrow or the data may not exist."
        result["explanation"] = explanation
    except Exception as e:
        result["explanation"] = f"Results retrieved but explanation failed: {str(e)}"

    return result


# ─── Quick Test (run directly) ───────────────────────────────────────────────

if __name__ == "__main__":
    # Test SQL generation without DB connection
    test_questions = [
        "Who are the top 10 golfers by composite score?",
        "Which golfers have the best strokes gained tee-to-green over their last 10 rounds?",
        "Show me players with a top 10 finish rate above 20% last year",
        "Who won the most prize money in major tournaments last year?",
    ]

    print("=== Testing NL → SQL Generation ===\n")
    for q in test_questions:
        print(f"Q: {q}")
        sql = generate_sql(q)
        print(f"SQL: {sql}\n")
        print("-" * 60)
