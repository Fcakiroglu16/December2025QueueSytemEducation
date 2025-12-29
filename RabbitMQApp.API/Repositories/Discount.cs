namespace RabbitMQApp.API.Repositories;

public class Discount
{
    public int Id { get; set; }
    public int UserId { get; set; }

    public double Rate { get; set; }

    public bool IsUsed { get; set; }
}