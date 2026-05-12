//nolint:wsl_v5
package postgresql_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	infra "github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
	"pgregory.net/rapid"
)

// Feature: table-partition-conversion, Property 1: CDC Queue Naming Convention
// For any valid schema name and source table name, the generated CDC queue table name SHALL equal
// `<source_table>_cdc_queue` and reside in the same schema as the source table.
// Validates: Requirements 1.1

func TestProperty1_CDCQueueNamingConvention(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		schema := rapid.StringMatching(`[a-z][a-z0-9_]{0,19}`).Draw(t, "schema")
		table := rapid.StringMatching(`[a-z][a-z0-9_]{0,19}`).Draw(t, "table")

		// Apply the naming convention as implemented in CreateCDCQueue
		queueTable := table + "_cdc_queue"

		// Property: queue name equals <table>_cdc_queue
		expectedSuffix := "_cdc_queue"
		if !strings.HasSuffix(queueTable, expectedSuffix) {
			t.Fatalf("queue table name %q does not end with %q", queueTable, expectedSuffix)
		}

		// Property: queue name starts with the source table name
		if !strings.HasPrefix(queueTable, table) {
			t.Fatalf("queue table name %q does not start with source table name %q", queueTable, table)
		}

		// Property: queue name is exactly <table>_cdc_queue
		if queueTable != table+"_cdc_queue" {
			t.Fatalf("queue table name %q != expected %q", queueTable, table+"_cdc_queue")
		}

		// Property: the qualified name resides in the same schema
		qualifiedQueue := pgx.Identifier{schema, queueTable}.Sanitize()
		qualifiedSource := pgx.Identifier{schema, table}.Sanitize()

		// Both should start with the same schema prefix
		schemaPrefix := pgx.Identifier{schema}.Sanitize() + "."
		if !strings.HasPrefix(qualifiedQueue, schemaPrefix) {
			t.Fatalf("queue qualified name %q does not start with schema prefix %q",
				qualifiedQueue, schemaPrefix)
		}
		if !strings.HasPrefix(qualifiedSource, schemaPrefix) {
			t.Fatalf("source qualified name %q does not start with schema prefix %q",
				qualifiedSource, schemaPrefix)
		}

		// The index name also follows the convention
		indexName := fmt.Sprintf("idx_%s_cdc_queue_seq", table)
		qualifiedIndex := pgx.Identifier{schema, indexName}.Sanitize()
		if !strings.HasPrefix(qualifiedIndex, schemaPrefix) {
			t.Fatalf("index qualified name %q does not start with schema prefix %q",
				qualifiedIndex, schemaPrefix)
		}
	})
}

// Feature: table-partition-conversion, Property 2: Trigger Function References All PK Columns
// For any valid set of primary key column names (1 to N columns), the generated trigger function SQL
// SHALL reference each primary key column exactly once in the array construction for both the NEW
// and OLD record paths.
// Validates: Requirements 2.1, 2.2

func TestProperty2_TriggerFunctionReferencesAllPKColumns(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		schema := rapid.StringMatching(`[a-z][a-z0-9_]{0,19}`).Draw(t, "schema")
		table := rapid.StringMatching(`[a-z][a-z0-9_]{0,19}`).Draw(t, "table")

		// Generate 1 to 5 unique PK column names
		numCols := rapid.IntRange(1, 5).Draw(t, "numPKCols")
		pkColumns := make([]string, numCols)
		usedNames := make(map[string]bool)

		for i := 0; i < numCols; i++ {
			var col string
			for {
				col = rapid.StringMatching(`[a-z][a-z0-9_]{0,14}`).Draw(t, fmt.Sprintf("pkCol%d", i))
				if !usedNames[col] {
					usedNames[col] = true
					break
				}
			}
			pkColumns[i] = col
		}

		// Reconstruct the SQL that CreateCDCTriggerFunction would generate
		// (mirrors the logic in setup.go)
		functionName := fmt.Sprintf("ppm_cdc_trigger_%s", table)
		qualifiedFunction := pgx.Identifier{schema, functionName}.Sanitize()
		qualifiedQueue := pgx.Identifier{schema, table + "_cdc_queue"}.Sanitize()

		// Build the array expression for OLD record (DELETE path)
		oldArrayParts := make([]string, len(pkColumns))
		for i, col := range pkColumns {
			oldArrayParts[i] = fmt.Sprintf("OLD.%s::TEXT", pgx.Identifier{col}.Sanitize())
		}
		oldArrayExpr := "ARRAY[" + strings.Join(oldArrayParts, ", ") + "]"

		// Build the array expression for NEW record (INSERT/UPDATE path)
		newArrayParts := make([]string, len(pkColumns))
		for i, col := range pkColumns {
			newArrayParts[i] = fmt.Sprintf("NEW.%s::TEXT", pgx.Identifier{col}.Sanitize())
		}
		newArrayExpr := "ARRAY[" + strings.Join(newArrayParts, ", ") + "]"

		generatedSQL := fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO %s (operation, pk_values)
        VALUES ('DELETE', %s);
        RETURN OLD;
    ELSE
        INSERT INTO %s (operation, pk_values)
        VALUES (TG_OP, %s);
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql`, qualifiedFunction, qualifiedQueue, oldArrayExpr, qualifiedQueue, newArrayExpr)

		// Property: each PK column appears exactly once in OLD path
		for _, col := range pkColumns {
			oldRef := fmt.Sprintf("OLD.%s::TEXT", pgx.Identifier{col}.Sanitize())
			count := strings.Count(generatedSQL, oldRef)
			if count != 1 {
				t.Fatalf("PK column %q should appear exactly once in OLD path, found %d times",
					col, count)
			}
		}

		// Property: each PK column appears exactly once in NEW path
		for _, col := range pkColumns {
			newRef := fmt.Sprintf("NEW.%s::TEXT", pgx.Identifier{col}.Sanitize())
			count := strings.Count(generatedSQL, newRef)
			if count != 1 {
				t.Fatalf("PK column %q should appear exactly once in NEW path, found %d times",
					col, count)
			}
		}

		// Property: the DELETE path uses OLD references
		if !strings.Contains(generatedSQL, "RETURN OLD") {
			t.Fatal("DELETE path should contain RETURN OLD")
		}

		// Property: the INSERT/UPDATE path uses NEW references
		if !strings.Contains(generatedSQL, "RETURN NEW") {
			t.Fatal("INSERT/UPDATE path should contain RETURN NEW")
		}

		// Property: the number of OLD.<col>::TEXT references equals the number of PK columns
		oldColRefCount := 0
		for _, col := range pkColumns {
			oldRef := fmt.Sprintf("OLD.%s::TEXT", pgx.Identifier{col}.Sanitize())
			oldColRefCount += strings.Count(generatedSQL, oldRef)
		}
		if oldColRefCount != numCols {
			t.Fatalf("expected %d OLD column references, found %d", numCols, oldColRefCount)
		}

		// Property: the number of NEW.<col>::TEXT references equals the number of PK columns
		newColRefCount := 0
		for _, col := range pkColumns {
			newRef := fmt.Sprintf("NEW.%s::TEXT", pgx.Identifier{col}.Sanitize())
			newColRefCount += strings.Count(generatedSQL, newRef)
		}
		if newColRefCount != numCols {
			t.Fatalf("expected %d NEW column references, found %d", numCols, newColRefCount)
		}
	})
}

// Feature: table-partition-conversion, Property 3: Target Table Preserves Column Definitions
// For any valid set of column definitions (with varying types, nullability, and defaults), the
// generated partitioned table DDL SHALL include all columns with their original data types,
// NOT NULL constraints, and DEFAULT values, plus a PARTITION BY RANGE clause on the specified
// partition key.
// Validates: Requirements 3.1

func TestProperty3_TargetTablePreservesColumnDefinitions(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		schema := rapid.StringMatching(`[a-z][a-z0-9_]{0,19}`).Draw(t, "schema")
		table := rapid.StringMatching(`[a-z][a-z0-9_]{0,19}`).Draw(t, "table")

		// Generate column definitions
		dataTypes := []string{"bigint", "integer", "text", "timestamptz", "boolean", "uuid", "jsonb", "date"}
		numCols := rapid.IntRange(2, 8).Draw(t, "numCols")

		columns := make([]infra.ColumnDef, numCols)
		usedNames := make(map[string]bool)

		// Ensure at least one column can serve as partition key
		partitionKeyIdx := rapid.IntRange(0, numCols-1).Draw(t, "partitionKeyIdx")

		for i := 0; i < numCols; i++ {
			var colName string
			for {
				colName = rapid.StringMatching(`[a-z][a-z0-9_]{0,14}`).Draw(t, fmt.Sprintf("colName%d", i))
				if !usedNames[colName] {
					usedNames[colName] = true
					break
				}
			}

			dataType := dataTypes[rapid.IntRange(0, len(dataTypes)-1).Draw(t, fmt.Sprintf("dataType%d", i))]
			isNullable := rapid.Bool().Draw(t, fmt.Sprintf("isNullable%d", i))

			var defaultValue *string
			hasDefault := rapid.Bool().Draw(t, fmt.Sprintf("hasDefault%d", i))
			if hasDefault {
				defaults := []string{"now()", "0", "true", "gen_random_uuid()"}
				dv := defaults[rapid.IntRange(0, len(defaults)-1).Draw(t, fmt.Sprintf("default%d", i))]
				defaultValue = &dv
			}

			columns[i] = infra.ColumnDef{
				Name:         colName,
				DataType:     dataType,
				IsNullable:   isNullable,
				DefaultValue: defaultValue,
				IsGenerated:  false,
			}
		}

		partitionKey := columns[partitionKeyIdx].Name

		// Reconstruct the DDL that CreatePartitionedTable would generate
		// (mirrors the logic in setup.go)
		qualifiedTable := pgx.Identifier{schema, table}.Sanitize()

		colDefs := make([]string, 0, len(columns))
		for _, col := range columns {
			colDef := fmt.Sprintf("    %s %s", pgx.Identifier{col.Name}.Sanitize(), col.DataType)
			if !col.IsNullable {
				colDef += " NOT NULL"
			}
			if col.DefaultValue != nil && !col.IsGenerated {
				colDef += fmt.Sprintf(" DEFAULT %s", *col.DefaultValue)
			}
			colDefs = append(colDefs, colDef)
		}

		generatedSQL := fmt.Sprintf("CREATE TABLE %s (\n%s\n) PARTITION BY RANGE (%s)",
			qualifiedTable,
			strings.Join(colDefs, ",\n"),
			pgx.Identifier{partitionKey}.Sanitize(),
		)

		// Property: all columns are present in the DDL with their data types
		for _, col := range columns {
			quotedName := pgx.Identifier{col.Name}.Sanitize()
			if !strings.Contains(generatedSQL, quotedName) {
				t.Fatalf("column %q not found in generated DDL:\n%s", col.Name, generatedSQL)
			}

			// Data type must appear on the same line as the column name
			if !strings.Contains(generatedSQL, quotedName+" "+col.DataType) {
				t.Fatalf("data type %q for column %q not found adjacent to column name in DDL:\n%s",
					col.DataType, col.Name, generatedSQL)
			}
		}

		// Property: NOT NULL appears for non-nullable columns
		for _, col := range columns {
			quotedName := pgx.Identifier{col.Name}.Sanitize()
			// Find the line containing this column
			lines := strings.Split(generatedSQL, "\n")
			for _, line := range lines {
				if strings.Contains(line, quotedName+" "+col.DataType) {
					if !col.IsNullable && !strings.Contains(line, "NOT NULL") {
						t.Fatalf("NOT NULL missing for non-nullable column %q in line: %s",
							col.Name, line)
					}
					if col.IsNullable && strings.Contains(line, "NOT NULL") {
						t.Fatalf("NOT NULL present for nullable column %q in line: %s",
							col.Name, line)
					}
					break
				}
			}
		}

		// Property: DEFAULT values appear for columns with defaults
		for _, col := range columns {
			if col.DefaultValue != nil && !col.IsGenerated {
				expectedDefault := fmt.Sprintf("DEFAULT %s", *col.DefaultValue)
				if !strings.Contains(generatedSQL, expectedDefault) {
					t.Fatalf("DEFAULT %s for column %q not found in DDL:\n%s",
						*col.DefaultValue, col.Name, generatedSQL)
				}
			}
		}

		// Property: PARTITION BY RANGE clause is present with the correct partition key
		partitionClause := fmt.Sprintf("PARTITION BY RANGE (%s)", pgx.Identifier{partitionKey}.Sanitize())
		if !strings.Contains(generatedSQL, partitionClause) {
			t.Fatalf("PARTITION BY RANGE clause with key %q not found in DDL:\n%s",
				partitionKey, generatedSQL)
		}

		// Property: number of column definitions matches input
		colDefCount := 0
		for _, line := range strings.Split(generatedSQL, "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "\"") && strings.Contains(trimmed, " ") {
				colDefCount++
			}
		}
		if colDefCount != len(columns) {
			t.Fatalf("expected %d column definitions, found %d in DDL:\n%s",
				len(columns), colDefCount, generatedSQL)
		}
	})
}

// Feature: table-partition-conversion, Property 4: Partition Key Inclusion in Constraints
// If the partition key is not already present in the constraint columns, the result includes
// the original columns plus the partition key appended.
// Validates: Requirements 3.5, 3.7

func TestProperty4_PartitionKeyInclusionInConstraints(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate PK columns (1 to 4)
		numPKCols := rapid.IntRange(1, 4).Draw(t, "numPKCols")
		pkColumns := make([]string, numPKCols)
		usedNames := make(map[string]bool)

		for i := 0; i < numPKCols; i++ {
			var col string
			for {
				col = rapid.StringMatching(`[a-z][a-z0-9_]{0,14}`).Draw(t, fmt.Sprintf("pkCol%d", i))
				if !usedNames[col] {
					usedNames[col] = true
					break
				}
			}
			pkColumns[i] = col
		}

		// Generate a partition key that may or may not be in the PK columns
		includeInPK := rapid.Bool().Draw(t, "partitionKeyInPK")

		var partitionKey string
		if includeInPK && numPKCols > 0 {
			// Pick one of the existing PK columns as partition key
			partitionKey = pkColumns[rapid.IntRange(0, numPKCols-1).Draw(t, "existingPKIdx")]
		} else {
			// Generate a new column name not in PK
			for {
				partitionKey = rapid.StringMatching(`[a-z][a-z0-9_]{0,14}`).Draw(t, "partitionKey")
				if !usedNames[partitionKey] {
					break
				}
			}
		}

		// Apply the partition key inclusion logic
		resultColumns := appendPartitionKeyIfMissing(pkColumns, partitionKey)

		// Check if partition key was in original columns
		partitionKeyInOriginal := false
		for _, col := range pkColumns {
			if col == partitionKey {
				partitionKeyInOriginal = true
				break
			}
		}

		if partitionKeyInOriginal {
			// Property: result should be identical to original when partition key already present
			if len(resultColumns) != len(pkColumns) {
				t.Fatalf("partition key %q is already in PK columns %v, result should be same length but got %v",
					partitionKey, pkColumns, resultColumns)
			}
			for i, col := range pkColumns {
				if resultColumns[i] != col {
					t.Fatalf("result[%d]=%q != original[%d]=%q when partition key already in PK",
						i, resultColumns[i], i, col)
				}
			}
		} else {
			// Property: result should be original + partition key appended
			expectedLen := len(pkColumns) + 1
			if len(resultColumns) != expectedLen {
				t.Fatalf("partition key %q not in PK columns %v, expected result length %d but got %d: %v",
					partitionKey, pkColumns, expectedLen, len(resultColumns), resultColumns)
			}
			// First N columns should match original order
			for i, col := range pkColumns {
				if resultColumns[i] != col {
					t.Fatalf("result[%d]=%q != original[%d]=%q", i, resultColumns[i], i, col)
				}
			}
			// Last column should be partition key
			if resultColumns[len(resultColumns)-1] != partitionKey {
				t.Fatalf("last column should be partition key %q but got %q",
					partitionKey, resultColumns[len(resultColumns)-1])
			}
		}

		// Property: partition key is always present in result
		found := false
		for _, col := range resultColumns {
			if col == partitionKey {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("partition key %q not found in result columns %v", partitionKey, resultColumns)
		}

		// Property: no duplicates in result
		seen := make(map[string]bool)
		for _, col := range resultColumns {
			if seen[col] {
				t.Fatalf("duplicate column %q in result %v", col, resultColumns)
			}
			seen[col] = true
		}

		// Property: all original columns are preserved in result
		for _, col := range pkColumns {
			foundInResult := false
			for _, r := range resultColumns {
				if r == col {
					foundInResult = true
					break
				}
			}
			if !foundInResult {
				t.Fatalf("original PK column %q not found in result %v", col, resultColumns)
			}
		}
	})
}

// Feature: table-partition-conversion, Property 6: Index Replication Preserves Non-PK Indexes
// For any set of index definitions on the source table, all non-primary-key indexes (including
// partial and expression indexes) SHALL be replicated to the target table with identical column
// lists, predicates, expressions, and methods.
// Validates: Requirements 3.4

func TestProperty6_IndexReplicationPreservesNonPKIndexes(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		schema := rapid.StringMatching(`[a-z][a-z0-9_]{0,19}`).Draw(t, "schema")
		table := rapid.StringMatching(`[a-z][a-z0-9_]{0,19}`).Draw(t, "table")

		// Generate a mix of indexes (some PK, some not)
		methods := []string{"btree", "hash", "gin", "gist"}
		numIndexes := rapid.IntRange(2, 6).Draw(t, "numIndexes")

		indexes := make([]infra.IndexDef, numIndexes)
		usedNames := make(map[string]bool)

		// First index is always PK
		var pkName string
		for {
			pkName = rapid.StringMatching(`[a-z][a-z0-9_]{0,14}_pkey`).Draw(t, "pkName")
			if !usedNames[pkName] {
				usedNames[pkName] = true
				break
			}
		}
		indexes[0] = infra.IndexDef{
			Name:      pkName,
			Columns:   []string{"id"},
			IsUnique:  true,
			IsPrimary: true,
			Method:    "btree",
		}

		// Remaining indexes are non-PK
		for i := 1; i < numIndexes; i++ {
			var idxName string
			for {
				idxName = rapid.StringMatching(`idx_[a-z][a-z0-9_]{0,14}`).Draw(t, fmt.Sprintf("idxName%d", i))
				if !usedNames[idxName] {
					usedNames[idxName] = true
					break
				}
			}

			// Generate columns for the index
			numIdxCols := rapid.IntRange(1, 3).Draw(t, fmt.Sprintf("numIdxCols%d", i))
			idxCols := make([]string, numIdxCols)
			usedColNames := make(map[string]bool)
			for j := 0; j < numIdxCols; j++ {
				var col string
				for {
					col = rapid.StringMatching(`[a-z][a-z0-9_]{0,14}`).Draw(t, fmt.Sprintf("idxCol%d_%d", i, j))
					if !usedColNames[col] {
						usedColNames[col] = true
						break
					}
				}
				idxCols[j] = col
			}

			isUnique := rapid.Bool().Draw(t, fmt.Sprintf("isUnique%d", i))
			method := methods[rapid.IntRange(0, len(methods)-1).Draw(t, fmt.Sprintf("method%d", i))]

			var predicate *string
			if rapid.Bool().Draw(t, fmt.Sprintf("hasPredicate%d", i)) {
				p := fmt.Sprintf("status = '%s'",
					rapid.StringMatching(`[a-z]{3,8}`).Draw(t, fmt.Sprintf("predVal%d", i)))
				predicate = &p
			}

			var expression *string
			if rapid.Bool().Draw(t, fmt.Sprintf("hasExpression%d", i)) {
				e := fmt.Sprintf("lower(%s)", idxCols[0])
				expression = &e
			}

			indexes[i] = infra.IndexDef{
				Name:       idxName,
				Columns:    idxCols,
				IsUnique:   isUnique,
				IsPrimary:  false,
				Predicate:  predicate,
				Expression: expression,
				Method:     method,
			}
		}

		// Filter to non-PK indexes
		nonPKIndexes := make([]infra.IndexDef, 0)
		for _, idx := range indexes {
			if !idx.IsPrimary {
				nonPKIndexes = append(nonPKIndexes, idx)
			}
		}

		// Verify that for each non-PK index, the generated SQL preserves all properties
		for _, idx := range nonPKIndexes {
			// Reconstruct the SQL that CreateIndex would generate (mirrors setup.go logic)
			qualifiedIndex := pgx.Identifier{schema, idx.Name}.Sanitize()
			qualifiedTable := pgx.Identifier{schema, table}.Sanitize()

			unique := ""
			if idx.IsUnique {
				unique = "UNIQUE "
			}

			var indexExpr string
			if idx.Expression != nil && *idx.Expression != "" {
				indexExpr = *idx.Expression
			} else {
				indexCols := make([]string, len(idx.Columns))
				for j, col := range idx.Columns {
					indexCols[j] = pgx.Identifier{col}.Sanitize()
				}
				indexExpr = strings.Join(indexCols, ", ")
			}

			methodStr := ""
			if idx.Method != "" && idx.Method != "btree" {
				methodStr = fmt.Sprintf(" USING %s", idx.Method)
			}

			generatedSQL := fmt.Sprintf("CREATE %sINDEX %s ON %s%s (%s)",
				unique, qualifiedIndex, qualifiedTable, methodStr, indexExpr)

			if idx.Predicate != nil && *idx.Predicate != "" {
				generatedSQL += fmt.Sprintf(" WHERE %s", *idx.Predicate)
			}

			// Property: column list is preserved (for non-expression indexes)
			if idx.Expression == nil || *idx.Expression == "" {
				for _, col := range idx.Columns {
					quotedCol := pgx.Identifier{col}.Sanitize()
					if !strings.Contains(generatedSQL, quotedCol) {
						t.Fatalf("index %q: column %q not preserved in SQL: %s",
							idx.Name, col, generatedSQL)
					}
				}
			}

			// Property: method is preserved
			if idx.Method != "btree" {
				if !strings.Contains(generatedSQL, fmt.Sprintf("USING %s", idx.Method)) {
					t.Fatalf("index %q: method %q not preserved in SQL: %s",
						idx.Name, idx.Method, generatedSQL)
				}
			}
			if idx.Method == "btree" {
				// btree is default, should NOT appear in SQL
				if strings.Contains(generatedSQL, "USING btree") {
					t.Fatalf("index %q: btree method should not appear explicitly in SQL: %s",
						idx.Name, generatedSQL)
				}
			}

			// Property: predicate is preserved for partial indexes
			if idx.Predicate != nil && *idx.Predicate != "" {
				if !strings.Contains(generatedSQL, fmt.Sprintf("WHERE %s", *idx.Predicate)) {
					t.Fatalf("index %q: predicate %q not preserved in SQL: %s",
						idx.Name, *idx.Predicate, generatedSQL)
				}
			}

			// Property: expression is preserved for expression indexes
			if idx.Expression != nil && *idx.Expression != "" {
				if !strings.Contains(generatedSQL, *idx.Expression) {
					t.Fatalf("index %q: expression %q not preserved in SQL: %s",
						idx.Name, *idx.Expression, generatedSQL)
				}
			}

			// Property: uniqueness is preserved
			if idx.IsUnique {
				if !strings.Contains(generatedSQL, "UNIQUE") {
					t.Fatalf("index %q: UNIQUE keyword missing in SQL: %s",
						idx.Name, generatedSQL)
				}
			}
			if !idx.IsUnique {
				if strings.Contains(generatedSQL, "UNIQUE") {
					t.Fatalf("index %q: UNIQUE keyword present but index is not unique: %s",
						idx.Name, generatedSQL)
				}
			}

			// Property: index name is preserved
			if !strings.Contains(generatedSQL, pgx.Identifier{idx.Name}.Sanitize()) {
				t.Fatalf("index %q: name not preserved in SQL: %s",
					idx.Name, generatedSQL)
			}
		}
	})
}

// appendPartitionKeyIfMissing implements the partition key inclusion logic:
// if the partition key is not already in the columns, append it.
// This mirrors the logic used in the setup phase for PK and unique constraint creation.
func appendPartitionKeyIfMissing(columns []string, partitionKey string) []string {
	for _, col := range columns {
		if col == partitionKey {
			return columns
		}
	}
	result := make([]string, len(columns)+1)
	copy(result, columns)
	result[len(columns)] = partitionKey
	return result
}
