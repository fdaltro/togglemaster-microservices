package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/joho/godotenv"
	"github.com/uptrace/opentelemetry-go-extra/otelsql"

	// Imports do OpenTelemetry (Traces + Metrics)
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

type App struct {
	DB        *sql.DB
	MasterKey string
}

func main() {
	_ = godotenv.Load()

	// --- 1. INICIALIZAÇÃO DO OPENTELEMETRY (TRACES) ---
	tp, err := initTracer()
	if err != nil {
		log.Fatalf("Falha ao inicializar o OpenTelemetry Tracer: %v", err)
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Erro ao desligar TracerProvider: %v", err)
		}
	}()

	// --- 2. INICIALIZAÇÃO DO OPENTELEMETRY (METRICS) ---
	mp, err := initMeter()
	if err != nil {
		log.Fatalf("Falha ao inicializar o OpenTelemetry Metrics: %v", err)
	}
	defer func() {
		if err := mp.Shutdown(context.Background()); err != nil {
			log.Printf("Erro ao desligar MeterProvider: %v", err)
		}
	}()

	// --- 3. CONFIGURAÇÃO ---
	port := os.Getenv("PORT")
	if port == "" {
		port = "8001"
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL deve ser definida")
	}

	masterKey := os.Getenv("MASTER_KEY")
	if masterKey == "" {
		log.Fatal("MASTER_KEY deve ser definida")
	}

	// --- 4. CONEXÃO COM O BANCO (INSTRUMENTADA) ---
	db, err := connectDB(databaseURL)
	if err != nil {
		log.Fatalf("Não foi possível conectar ao banco de dados: %v", err)
	}
	defer db.Close()

	app := &App{
		DB:        db,
		MasterKey: masterKey,
	}

	// --- 5. ROTAS ---
	mux := http.NewServeMux()
	// O compilador do Go vai buscar essas funções automaticamente no seu arquivo handlers.go
	mux.HandleFunc("/health", app.healthHandler)
	mux.HandleFunc("/validate", app.validateKeyHandler)
	mux.Handle("/admin/keys", app.masterKeyAuthMiddleware(http.HandlerFunc(app.createKeyHandler)))

	// --- 6. ENVELOPAMENTO HTTP ---
	handler := otelhttp.NewHandler(mux, "auth-service-http")

	log.Printf("Serviço de Autenticação (Go) rodando na porta %s", port)
	// nosemgrep: go.lang.security.audit.net.use-tls.use-tls
	if err := http.ListenAndServe(":"+port, handler); err != nil {
		log.Fatal(err)
	}
}

// --- FUNÇÕES DE APOIO -----

func connectDB(databaseURL string) (*sql.DB, error) {
	db, err := otelsql.Open("pgx", databaseURL,
		otelsql.WithAttributes(semconv.DBSystemPostgreSQL),
		otelsql.WithDBName("auth-db"),
	)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}

func initTracer() (*sdktrace.TracerProvider, error) {
	ctx := context.Background()
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithProcess(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return tp, nil
}

// 🔥 NOVA FUNÇÃO: Inicializa a geração de métricas numéricas (APM)
func initMeter() (*metric.MeterProvider, error) {
	ctx := context.Background()
	
	// Cria o exportador OTLP via gRPC para a porta 4317
	exporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithProcess(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
	)
	if err != nil {
		return nil, err
	}

	// Cria o provedor que fará a leitura periódica das métricas do otelhttp e enviará ao Collector
	mp := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(exporter)),
		metric.WithResource(res),
	)

	otel.SetMeterProvider(mp)
	return mp, nil
}