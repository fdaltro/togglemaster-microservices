package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/joho/godotenv"

	// Novos imports do OpenTelemetry
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	//semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

// App struct (para injeção de dependência)
type App struct {
	DB        *sql.DB
	MasterKey string
}

func main() {
	// Carrega o .env para desenvolvimento local.
	_ = godotenv.Load()

	// --- INICIALIZAÇÃO DO OPENTELEMETRY ---
	// O Tracer vai ler as variáveis OTEL_EXPORTER_OTLP_ENDPOINT e OTEL_SERVICE_NAME automaticamente do Kubernetes
	tp, err := initTracer()
	if err != nil {
		log.Fatalf("Falha ao inicializar o OpenTelemetry: %v", err)
	}
	// Garante que todos os traces sejam enviados antes do app desligar
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Erro ao desligar TracerProvider: %v", err)
		}
	}()

	// --- Configuração ---
	port := os.Getenv("PORT")
	if port == "" {
		port = "8001" // Porta padrão
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL deve ser definida")
	}

	masterKey := os.Getenv("MASTER_KEY")
	if masterKey == "" {
		log.Fatal("MASTER_KEY deve ser definida")
	}

	// --- Conexão com o Banco ---
	db, err := connectDB(databaseURL)
	if err != nil {
		log.Fatalf("Não foi possível conectar ao banco de dados: %v", err)
	}
	defer db.Close()

	app := &App{
		DB:        db,
		MasterKey: masterKey,
	}

	// --- Rotas da API ---
	mux := http.NewServeMux()
	mux.HandleFunc("/health", app.healthHandler)
	mux.HandleFunc("/validate", app.validateKeyHandler)
	mux.Handle("/admin/keys", app.masterKeyAuthMiddleware(http.HandlerFunc(app.createKeyHandler)))

	// --- ENVELOPAMENTO OPENTELEMETRY ---
	// Aqui a mágica acontece: envolvemos o roteador padrão (mux) com o otelhttp.
	// Isso fará com que todas as requisições gerem um Trace automaticamente!
	handler := otelhttp.NewHandler(mux, "auth-service-http")

	log.Printf("Serviço de Autenticação (Go) rodando na porta %s", port)
	// Subimos o servidor usando o handler envelopado em vez do mux puro
	// nosemgrep: go.lang.security.audit.net.use-tls.use-tls
	if err := http.ListenAndServe(":"+port, handler); err != nil {
		log.Fatal(err)
	}
}

// connectDB inicializa e testa a conexão com o PostgreSQL
func connectDB(databaseURL string) (*sql.DB, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	log.Println("Conectado ao PostgreSQL com sucesso!")
	return db, nil
}

// initTracer configura o agente do OpenTelemetry
func initTracer() (*sdktrace.TracerProvider, error) {
	ctx := context.Background()

	// Cria um exportador gRPC inseguro (perfeito para rede interna do Kubernetes)
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	// Captura os dados do ambiente (como OTEL_SERVICE_NAME definido no ArgoCD)
	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithProcess(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
	)
	if err != nil {
		return nil, err
	}

	// Cria o provedor de Traces com exportação em lote
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	// Define o provedor como global na aplicação
	otel.SetTracerProvider(tp)
	// Propaga o contexto entre microsserviços (ex: se o Auth chamar o Evaluation, o Trace segue junto)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return tp, nil
}