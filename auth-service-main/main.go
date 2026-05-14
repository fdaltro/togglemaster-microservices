package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/joho/godotenv"
	"github.com/uptrace/opentelemetry-go-extra/otelsql"

	// Imports do OpenTelemetry
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

type App struct {
	DB        *sql.DB
	MasterKey string
}

// Estruturas de dados originais do seu projeto
type KeyRequest struct {
	Key string `json:"key"`
}

type KeyResponse struct {
	Valid bool `json:"valid"`
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
	if port == "" { port = "8001" }

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" { log.Fatal("DATABASE_URL deve ser definida") }

	masterKey := os.Getenv("MASTER_KEY")
	if masterKey == "" { log.Fatal("MASTER_KEY deve ser definida") }

	// --- 3. CONEXÃO COM O BANCO (INSTRUMENTADA) ---
	db, err := connectDB(databaseURL)
	if err != nil {
		log.Fatalf("Não foi possível conectar ao banco de dados: %v", err)
	}
	defer db.Close()

	app := &App{
		DB:        db,
		MasterKey: masterKey,
	}

	// --- 4. ROTAS ---
	mux := http.NewServeMux()
	mux.HandleFunc("/health", app.healthHandler)
	mux.HandleFunc("/validate", app.validateKeyHandler)
	mux.Handle("/admin/keys", app.masterKeyAuthMiddleware(http.HandlerFunc(app.createKeyHandler)))

	// --- 5. ENVELOPAMENTO HTTP ---
	handler := otelhttp.NewHandler(mux, "auth-service-http")

	log.Printf("Serviço de Autenticação (Go) rodando na porta %s", port)
	if err := http.ListenAndServe(":"+port, handler); err != nil {
		log.Fatal(err)
	}
}

// --- FUNÇÕES DE APOIO ---

func connectDB(databaseURL string) (*sql.DB, error) {
	db, err := otelsql.Open("pgx", databaseURL,
		otelsql.WithAttributes(semconv.DBSystemPostgreSQL),
		otelsql.WithDBName("auth-db"),
	)
	if err != nil { return nil, err }
	if err = db.Ping(); err != nil { return nil, err }
	return db, nil
}

func initTracer() (*sdktrace.TracerProvider, error) {
	ctx := context.Background()
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithInsecure())
	if err != nil { return nil, err }

	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithProcess(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
	)
	if err != nil { return nil, err }

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return tp, nil
}

// --- OS HANDLERS (A LÓGICA DO SEU SERVIÇO) ---

func (app *App) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (app *App) validateKeyHandler(w http.ResponseWriter, r *http.Request) {
	var req KeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	var exists bool
	query := "SELECT EXISTS(SELECT 1 FROM api_keys WHERE key = $1)"
	// O otelsql vai capturar essa query automaticamente!
	err := app.DB.QueryRowContext(r.Context(), query, req.Key).Scan(&exists)
	if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(KeyResponse{Valid: exists})
}

func (app *App) createKeyHandler(w http.ResponseWriter, r *http.Request) {
	var req KeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	_, err := app.DB.ExecContext(r.Context(), "INSERT INTO api_keys (key) VALUES ($1)", req.Key)
	if err != nil {
		http.Error(w, "Error creating key", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (app *App) masterKeyAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Master-Key") != app.MasterKey {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}