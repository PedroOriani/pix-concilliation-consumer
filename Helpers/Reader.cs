using System.IO;
using System.Text.Json;

public class Reader
{
    public static List<OutInfo> Read(string path)
    {
        List<OutInfo> transactions = []; // Inicialização correta da lista
        if (File.Exists(path))
        {
            using (StreamReader fileReader = new StreamReader(path))
            {
                string line;
                while ((line = fileReader.ReadLine()) != null)
                {
                    OutInfo transaction = JsonSerializer.Deserialize<OutInfo>(line);
                    if (transaction != null)
                    {
                        transactions.Add(transaction);
                    }
                    else
                    {
                        Console.WriteLine("Error in deserializing line");
                    }
                }
            }
            return transactions;
        }
        else
        {
            Console.WriteLine("File doesn't exist");
            throw new Exception();
        }
    }
}

