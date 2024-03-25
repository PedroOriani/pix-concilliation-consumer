using System.Data;
using System.Globalization;
using Npgsql;

public class Writer
{
    public async Task<List<OutInfo>> GetPaymentsByDateAndBank(string date, int bankId)
    {
        // var connString = "Host=localhost;Username=postgres;Password=2483;Database=pix"; //Local
        var connString = "Host=172.24.0.5;Username=postgres;Password=postgres;Database=pixAPI_docker"; //Docker
        using var postgresConnection = new NpgsqlConnection(connString);
        await postgresConnection.OpenAsync();

        DateTime formatedDate = DateTime.ParseExact(date, "yyyy-MM-dd'T'HH:mm:ss", CultureInfo.InvariantCulture);
        
        string sqlQuery = @"
            SELECT p.""Id"", p.""Status""
            FROM ""Payments"" AS p
            INNER JOIN ""Accounts"" AS a1 ON p.""PaymentProviderAccountId"" = a1.""Id""
            INNER JOIN ""Keys"" AS k ON p.""PixKeyId"" = k.""Id""
            INNER JOIN ""Accounts"" AS a2 ON k.""AccountId"" = a2.""Id""
            WHERE DATE_TRUNC('day', p.""CreatedAt"") = @date
            AND (a1.""BankId"" = @bankId OR a2.""BankId"" = @bankId)
            ORDER BY p.""Id""";

        using var command = new NpgsqlCommand(sqlQuery, postgresConnection);
        
        command.Parameters.AddWithValue("@date", formatedDate);
        command.Parameters.AddWithValue("@bankId", bankId);
        
        var payments = new List<OutInfo>();

        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var paymentInfo = new OutInfo
            {
                Id = reader.GetInt32("Id"),
                Status = reader.GetString("Status"),
            };

            payments.Add(paymentInfo);
        }
        return payments;
    }
}