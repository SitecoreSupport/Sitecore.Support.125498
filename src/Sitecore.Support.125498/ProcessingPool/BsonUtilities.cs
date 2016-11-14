
namespace Sitecore.Support.ListManager.ProcessingPool
{
  using System;
  using MongoDB.Bson;
  using MongoDB.Bson.IO;
  using MongoDB.Bson.Serialization;

  // Original behavior
  public static class BsonUtilities
  {
    private const string FIELD_NAME = "value";

    [NotNull]
    public static BsonValue ToBsonValue([NotNull] Type type, [NotNull] object value)
    {
      BsonDocument root = new BsonDocument();
      BsonDocumentWriterSettings settings = new BsonDocumentWriterSettings();

      using (BsonWriter writer = new BsonDocumentWriter(root, settings))
      {
        writer.WriteStartDocument();
        writer.WriteName(FIELD_NAME);

        BsonSerializer.Serialize(writer, type, value);
      }

      return root[0];
    }

    [NotNull]
    public static object FromBsonValue([NotNull] Type type, [NotNull] BsonValue value)
    {
      BsonDocument root = new BsonDocument();
      BsonDocumentReaderSettings settings = new BsonDocumentReaderSettings();

      root[FIELD_NAME] = value;

      object result = null;

      using (BsonReader reader = new BsonDocumentReader(root, settings))
      {
        reader.ReadStartDocument();
        reader.ReadName(FIELD_NAME);

        result = BsonSerializer.Deserialize(reader, type);
      }

      return result;
    }
  }
}
