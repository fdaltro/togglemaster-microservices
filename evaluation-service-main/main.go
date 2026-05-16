package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/go-redis/redis/extra/redisotel/v8"
	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"

	// Imports do OpenTelemetry
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// App unificada com todos os atributos esperados pelo evaluator.go e sqs.go
type App struct {
	RedisClient         *redis.Client
	HttpClient          *http.Client
	FlagServiceURL      string
	TargetingServiceURL string
	SqsSvc              *sqs.SQS
	SqsQueueURL         string
}

func main() {
	_ = godotenv.Load()

	// --- 1. INICIALIZAÇÃO DO OPENTELEMETRY ---
	tp, err := initTracer()
	if err != nil {
		log.Fatalf("Falha ao inicializar o OpenTelemetry: %v", err)
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Erro ao desligar TracerProvider: %v", err)
		}
	}()

	// --- 2. CONFIGURAÇÃO DE VARIÁVEIS ---
	port := os.Getenv("PORT")
	if port == "" {
		port = "8004" 
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	flagServiceURL := os.Getenv("FLAG_SERVICE_URL")
	if flagServiceURL == "" {
		flagServiceURL = "http://localhost:8002"
	}

	targetingServiceURL := os.Getenv("TARGETING_SERVICE_URL")
	if targetingServiceURL == "" {
		targetingServiceURL = "http://localhost:8003"
	}

	sqsQueueURL := os.Getenv("AWS_SQS_URL")

	// --- 3. CONEXÃO COM REDIS (INSTRUMENTADA) ---
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	rdb.AddHook(redisotel.NewTracingHook())

	// --- 4. AWS SQS CLIENT ---
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	sqsSvc := sqs.New(sess)

	// --- 5. CLIENTE HTTP (INSTRUMENTADO PARA CHAMADAS EXTERNAS) ---
	httpClient := &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
		Timeout:   5 * time.Second,
	}

	// --- 6. POPULANDO O APP ---
	app := &App{
		RedisClient:         rdb,
		HttpClient:          httpClient,
		FlagServiceURL:      flagServiceURL,
		TargetingServiceURL: targetingServiceURL,
		SqsSvc:              sqsSvc,
		SqsQueueURL:         sqsQueueURL,
	}

	// --- 7. ROTAS ---
	mux := http.NewServeMux()
	mux.HandleFunc("/health", app.healthHandler)
	mux.HandleFunc("/evaluate", app.evaluationHandler)

	// --- 8. ENVELOPAMENTO HTTP PARA RECEBER REQUISIÇÕES ---
	handler := otelhttp.NewHandler(mux, "evaluation-service-http")

	log.Printf("Serviço de Evaluation (Go) rodando na porta %s", port)
	// nosemgrep: go.lang.security.audit.net.use-tls.use-tls
	if err := http.ListenAndServe(":"+port, handler); err != nil {
		log.Fatal(err)
	}
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