package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

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

type App struct {
	RDB            *redis.Client
	HTTPClient     *http.Client
	FlagServiceURL string
}

type EvalRequest struct {
	FlagKey string `json:"flag_key"`
	User    string `json:"user"`
}

type EvalResponse struct {
	Enabled bool `json:"enabled"`
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

	// --- 2. CONFIGURAÇÃO ---
	port := os.Getenv("PORT")
	if port == "" {
		port = "8003" // Porta padrão do evaluation-service
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	flagServiceURL := os.Getenv("FLAG_SERVICE_URL")
	if flagServiceURL == "" {
		flagServiceURL = "http://localhost:8002"
	}

	// --- 3. CONEXÃO COM REDIS (INSTRUMENTADA) ---
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	rdb.AddHook(redisotel.NewTracingHook()) // <-- Hook mágico do OTel para o Redis

	// --- 4. CLIENTE HTTP (INSTRUMENTADO PARA CHAMADAS DE SAÍDA) ---
	httpClient := &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport), // <-- Repassa o Trace ID
		Timeout:   5 * time.Second,
	}

	app := &App{
		RDB:            rdb,
		HTTPClient:     httpClient,
		FlagServiceURL: flagServiceURL,
	}

	// --- 5. ROTAS ---
	mux := http.NewServeMux()
	mux.HandleFunc("/health", app.healthHandler)
	mux.HandleFunc("/evaluate", app.evaluateHandler)

	// --- 6. ENVELOPAMENTO HTTP (PARA CHAMADAS DE ENTRADA) ---
	handler := otelhttp.NewHandler(mux, "evaluation-service-http")

	log.Printf("Serviço de Evaluation (Go) rodando na porta %s", port)
	// nosemgrep: go.lang.security.audit.net.use-tls.use-tls
	if err := http.ListenAndServe(":"+port, handler); err != nil {
		log.Fatal(err)
	}
}

// --- FUNÇÕES DE APOIO -----

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

// --- OS HANDLERS ---

func (app *App) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (app *App) evaluateHandler(w http.ResponseWriter, r *http.Request) {
	var req EvalRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// 1. Tenta buscar no Redis usando o contexto do OTel
	cachedValue, err := app.RDB.Get(r.Context(), "flag:"+req.FlagKey).Result()
	if err == redis.Nil {
		// Cache Miss: Busca no Flag Service usando o cliente HTTP instrumentado
		// IMPORTANTE: NewRequestWithContext garante que a linha continue no mapa do Datadog
		apiReq, _ := http.NewRequestWithContext(r.Context(), "GET", app.FlagServiceURL+"/flags/"+req.FlagKey, nil)
		
		resp, err := app.HTTPClient.Do(apiReq)
		if err != nil {
			http.Error(w, "Error calling Flag Service", http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		// Lógica de fallback se não encontrar (exemplo simples)
		// Neste ponto você incluiria sua regra de negócio para ler a resposta e salvar no Redis
		json.NewEncoder(w).Encode(EvalResponse{Enabled: true}) // Mock de resposta
		return
	} else if err != nil {
		http.Error(w, "Redis error", http.StatusInternalServerError)
		return
	}

	// Retorna do Cache
	enabled := cachedValue == "true"
	json.NewEncoder(w).Encode(EvalResponse{Enabled: enabled})
}