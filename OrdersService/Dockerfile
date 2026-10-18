# Build context = raiz del repositorio (necesario para copiar tambien Contracts/).
#   docker build -f OrdersService/Dockerfile -t tp11-orders .
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY Contracts/ Contracts/
COPY OrdersService/ OrdersService/

RUN dotnet restore OrdersService/OrdersService.csproj
RUN dotnet publish OrdersService/OrdersService.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app/publish .
ENV ASPNETCORE_URLS=http://+:8080 \
    DOTNET_RUNNING_IN_CONTAINER=true
EXPOSE 8080
ENTRYPOINT ["dotnet", "OrdersService.dll"]