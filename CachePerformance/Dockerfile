#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["CachePerformance/*.csproj", "CachePerformance/"]
COPY ["Core/Cache/*.csproj", "Core/Cache/"]
COPY ["Core/Common/*.csproj", "Core/Common/"]
COPY ["Core/Model/*.csproj", "Core/Model/"]
COPY ["Core/Services/*.csproj", "Core/Services/"]
COPY ["Core/Repositories/*.csproj", "Core/Repositories/"]
RUN dotnet restore "CachePerformance/CachePerformance.csproj"
COPY . .
WORKDIR "/src/CachePerformance"
RUN dotnet build "CachePerformance.csproj" -c Release -o /app

FROM build AS publish
RUN dotnet publish "CachePerformance.csproj" -c Release -o /app

FROM base AS final
WORKDIR /app
COPY --from=publish /app .
ENTRYPOINT ["dotnet", "CachePerformance.dll"]