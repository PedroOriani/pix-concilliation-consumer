using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Data;

var builder = WebApplication.CreateBuilder(args);

var app = builder.Build();

app.UseHttpsRedirection();

var connectionFactory = new ConnectionFactory()
{
    HostName = "localhost",
    UserName = "guest",
    Password = "guest"
};

var queueName = "concilliations";

using var connection = connectionFactory.CreateConnection();
using var channel = connection.CreateModel();
using var client = new HttpClient();

channel.QueueDeclare(
    queue: queueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null
);

Console.WriteLine("[*] Waiting for messages...");

//FileUtil.CreateFile();

var writer = new Writer();

var consumer = new EventingBasicConsumer(channel);
consumer.Received += async (model, ea) =>
{
    var start = DateTime.Now;

    var body = ea.Body.ToArray();
    var jsonMessage = Encoding.UTF8.GetString(body);
    var concilliation = JsonSerializer.Deserialize<ConcilliationDTO>(jsonMessage);

    if (concilliation == null) channel.BasicReject(ea.DeliveryTag, requeue: false);

    Console.WriteLine("Received concilliation message: {0}", jsonMessage);

    try
    {
        var paymentsFile = Reader.Read(concilliation.File);
        var dbPayments = await writer.GetPaymentsByDateAndBank(concilliation.Date, concilliation.BankId);

        var paymentsFileSet = new HashSet<int>(paymentsFile.Select(p => p.Id));
        var dbPaymentsSet = new HashSet<int>(dbPayments.Select(p => p.Id));

        var databaseToFile = dbPayments
            .Where(p => !paymentsFileSet.Contains(p.Id))
            .ToList();

        var fileToDatabase = paymentsFile
            .Where(p => !dbPaymentsSet.Contains(p.Id))
            .ToList();

        var differentStatus = new List<DifferentStatusIds>();

        foreach (var databasePayment in dbPayments)
        {
            if (!paymentsFileSet.Contains(databasePayment.Id))
            {
                databaseToFile.Add(databasePayment);
            }
            else
            {
                var correspondingFilePayment = paymentsFile.FirstOrDefault(p => p.Id == databasePayment.Id);
                if (correspondingFilePayment != null && correspondingFilePayment.Status != databasePayment.Status)
                {
                    differentStatus.Add(new DifferentStatusIds { Id = databasePayment.Id });
                }
            }
        }


        var comparisonResult = new
        {
            databaseToFile,
            fileToDatabase,
            differentStatus
        };

        Console.WriteLine($"Enviando solicitação HTTP para: {concilliation.Postback}");
    
        await client.PostAsJsonAsync(concilliation.Postback, comparisonResult);

        var end = DateTime.Now;
        Console.WriteLine($"Execution time: {(end - start).TotalMilliseconds}ms");
        channel.BasicAck(ea.DeliveryTag, false);
    }
    catch (Exception e)
    {
        Console.WriteLine($"Concilliation failed with error: {e.Message}");
        Console.WriteLine($"Detalhes da exceção: {e.ToString()}");
        channel.BasicReject(ea.DeliveryTag, requeue: false);

        return;
    }
};

channel.BasicConsume(
    queue: queueName,
    autoAck: false,
    consumer: consumer
);

Console.WriteLine("Press [enter] to exit");
Console.ReadLine();

// Start the web application
app.Run();