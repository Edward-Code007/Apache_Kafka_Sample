# Build
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish ./Kafka_First_Aproach.Consumer/Kafka_First_Aproach.Consumer.csproj -c Release -o /app

# Runtime
FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
EXPOSE 13010
ENTRYPOINT ["dotnet", "consumer.dll"]

