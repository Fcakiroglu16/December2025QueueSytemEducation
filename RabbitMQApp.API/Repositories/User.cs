namespace RabbitMQApp.API.Repositories
{
    public class User
    {
        public int Id { get; set; }
        public required string UserName { get; set; }
        public required string Email { get; set; }
        public required string Phone { get; set; }
    }
}
