// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using ArrowStructType = Apache.Arrow.Types.StructType;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Column-mapping spec compliance: data files must store PHYSICAL column names (in BOTH id and name mode)
/// and parquet field ids, at EVERY nesting depth. Writing logical names in id mode, or leaving nested struct
/// children logical-named, makes the file read as all-NULLs for a spec reader (Spark, delta-kernel).
/// </summary>
public class ColumnMappingSpecComplianceTests : IDisposable
{
    private readonly string _tempDir;

    public ColumnMappingSpecComplianceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cmspec_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Apache.Arrow.Schema FlatSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Build();

    // struct<inner: int64, label: string> — the nested case the flat top-level renames used to miss.
    private static Apache.Arrow.Schema NestedSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("nested", new ArrowStructType(
            [
                new Field("inner", Int64Type.Default, true),
                new Field("label", StringType.Default, true),
            ]), true))
            .Build();

    private static RecordBatch NestedBatch(Apache.Arrow.Schema schema)
    {
        var ids = new Int64Array.Builder().Append(1).Append(2).Build();
        var inner = new Int64Array.Builder().Append(10).Append(20).Build();
        var labels = new StringArray.Builder().Append("a").Append("b").Build();
        var structType = (ArrowStructType)schema.FieldsList[1].DataType;
        var nested = new StructArray(structType, 2, [inner, labels], ArrowBuffer.Empty);
        return new RecordBatch(schema, [ids, nested], 2);
    }

    [Theory]
    [InlineData(ColumnMappingMode.Id)]
    [InlineData(ColumnMappingMode.Name)]
    public async Task DataFile_UsesPhysicalNames_InBothModes(ColumnMappingMode mode)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = FlatSchema();

        await using var table = await DeltaTable.CreateAsync(fs, schema, columnMappingMode: mode);

        var ids = new Int64Array.Builder().Append(1).Build();
        var values = new StringArray.Builder().Append("a").Build();
        await table.WriteAsync([new RecordBatch(schema, [ids, values], 1)]);

        var deltaSchema = table.CurrentSnapshot.Schema;
        var expected = deltaSchema.Fields
            .Select(f => ColumnMapping.GetPhysicalName(f, mode))
            .ToList();
        // Physical names are col-<guid> — never the logical ones.
        Assert.All(expected, p => Assert.StartsWith("col-", p));

        var addFile = table.CurrentSnapshot.ActiveFiles.Values.First();
        await using var file = await fs.OpenReadAsync(DeltaPath.Decode(addFile.Path));
        using var reader = new Parquet.ParquetFileReader(file, ownsFile: false);
        var parquetSchema = await reader.GetSchemaAsync();

        var actual = parquetSchema.Root.Children.Select(c => c.Element.Name).ToList();
        Assert.Equal(expected, actual);
        // Field ids are stamped in both modes (id-mode readers resolve by them).
        Assert.All(parquetSchema.Root.Children, c => Assert.True(c.Element.FieldId.HasValue));
    }

    [Theory]
    [InlineData(ColumnMappingMode.Id)]
    [InlineData(ColumnMappingMode.Name)]
    public async Task NestedStructChildren_UsePhysicalNamesAndFieldIds(ColumnMappingMode mode)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = NestedSchema();

        await using var table = await DeltaTable.CreateAsync(fs, schema, columnMappingMode: mode);
        await table.WriteAsync([NestedBatch(schema)]);

        var nestedField = table.CurrentSnapshot.Schema.Fields[1];
        var nestedStruct = (EngineeredWood.DeltaLake.Schema.StructType)nestedField.Type;
        var expectedChildren = nestedStruct.Fields
            .Select(f => ColumnMapping.GetPhysicalName(f, mode))
            .ToList();
        Assert.All(expectedChildren, p => Assert.StartsWith("col-", p));

        var addFile = table.CurrentSnapshot.ActiveFiles.Values.First();
        await using var file = await fs.OpenReadAsync(DeltaPath.Decode(addFile.Path));
        using var reader = new Parquet.ParquetFileReader(file, ownsFile: false);
        var parquetSchema = await reader.GetSchemaAsync();

        var nestedNode = parquetSchema.Root.Children
            .Single(c => c.Element.Name == ColumnMapping.GetPhysicalName(nestedField, mode));
        var actualChildren = nestedNode.Children.Select(c => c.Element.Name).ToList();
        Assert.Equal(expectedChildren, actualChildren);
        Assert.All(nestedNode.Children, c => Assert.True(c.Element.FieldId.HasValue));
    }

    [Theory]
    [InlineData(ColumnMappingMode.Id)]
    [InlineData(ColumnMappingMode.Name)]
    public async Task NestedStruct_RoundTripsToLogicalNames(ColumnMappingMode mode)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = NestedSchema();

        await using var table = await DeltaTable.CreateAsync(fs, schema, columnMappingMode: mode);
        await table.WriteAsync([NestedBatch(schema)]);

        var batches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            batches.Add(b);

        var read = Assert.Single(batches);
        Assert.Equal(2, read.Length);
        Assert.Equal("id", read.Schema.FieldsList[0].Name);
        Assert.Equal("nested", read.Schema.FieldsList[1].Name);

        // The nested children come back under their LOGICAL names, not col-<guid>.
        var readStruct = (StructArray)read.Column(1);
        var readStructType = (ArrowStructType)read.Schema.FieldsList[1].DataType;
        Assert.Equal(["inner", "label"], readStructType.Fields.Select(f => f.Name));

        Assert.Equal(10L, ((Int64Array)readStruct.Fields[0]).GetValue(0));
        Assert.Equal("b", ((StringArray)readStruct.Fields[1]).GetString(1));
    }
}
