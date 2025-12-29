using Microsoft.EntityFrameworkCore;

namespace RabbitMQApp.API.Repositories;

public class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<OutBox> OutBoxes { get; set; }

    public DbSet<Discount> Discounts { get; set; }

    public DbSet<Inbox> Inboxes { get; set; }

    public DbSet<Idempotency> Idempotencies { get; set; }

    public DbSet<User> Users { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Idempotency>(x =>
        {
            x.HasKey(x => x.Key);
            x.Property(x => x.Key).ValueGeneratedNever();
        });

        modelBuilder.Entity<OutBox>(x => { x.Property(x => x.EventData).HasMaxLength(1000); });

        base.OnModelCreating(modelBuilder);
    }
}