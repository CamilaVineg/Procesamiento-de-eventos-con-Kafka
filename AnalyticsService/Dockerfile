FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY Contracts/ Contracts/
COPY AnalyticsService/ AnalyticsService/

RUN dotnet restore AnalyticsService/AnalyticsService.csproj
RUN dotnet publish AnalyticsService/AnalyticsService.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app/publish .
ENV ASPNETCORE_URLS=http://+:8080 \
    DOTNET_RUNNING_IN_CONTAINER=true
EXPOSE 8080
ENTRYPOINT ["dotnet", "AnalyticsService.dll"]