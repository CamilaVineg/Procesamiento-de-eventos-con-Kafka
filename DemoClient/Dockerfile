FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY Contracts/ Contracts/
COPY DemoClient/ DemoClient/

RUN dotnet restore DemoClient/DemoClient.csproj
RUN dotnet publish DemoClient/DemoClient.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/runtime:8.0
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "DemoClient.dll"]