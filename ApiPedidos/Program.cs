var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();
builder.Services.AddControllers();

var app = builder.Build();
app.MapControllers();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}
app.Run();

