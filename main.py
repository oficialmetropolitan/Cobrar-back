from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

from database import create_pool, close_pool
from routes import cliente, contrato, parcela, dashboard, Onboarding, adiantamento, extrairpdf
from routes.webhook_btg import router as webhook_btg_router
from scheduler import criar_scheduler, job_cobrancas



# Configuração de Logs básica para ver o agendador no terminal
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

scheduler = criar_scheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Startup ---
    await create_pool()
    scheduler.start()
    
    # Log para confirmar o próximo disparo
    job = scheduler.get_job("job_cobrancas")
    if job:
        logger.info(f"SISTEMA ONLINE: Próxima rotina de cobrança agendada para: {job.next_run_time}")
    
    yield
    
    # --- Shutdown ---
    scheduler.shutdown()
    await close_pool()
    logger.info("SISTEMA OFFLINE: Agendador desligado.")

app = FastAPI(
    title="Metropolitan Cobrança API",
    lifespan=lifespan,
)

# ... (Configuração de CORS igual ao seu anterior) ...
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rotas
app.include_router(cliente.router, prefix="/clientes", tags=["Clientes"])
app.include_router(contrato.router, prefix="/contratos", tags=["Contratos"])
app.include_router(parcela.router, prefix="/parcelas", tags=["Parcelas"])
app.include_router(dashboard.router, prefix="/dashboard", tags=["Dashboard"])
app.include_router(Onboarding.router, prefix="/onboarding", tags=["Onboarding"])
app.include_router(adiantamento.router, prefix="/adiantamentos", tags=["Adiantamentos"])
app.include_router(extrairpdf.router, prefix="/api")
app.include_router(webhook_btg_router, prefix="", tags=["Webhook BTG"])

@app.get("/", tags=["Health"])
async def root():
    return {"status": "ok", "proximo_disparo": str(scheduler.get_job("job_cobrancas").next_run_time)}

@app.post("/admin/disparar-cobrancas", tags=["Admin"])
async def disparar_cobrancas_manual():
    # Executa a função imediatamente sem esperar as 08:00
    await job_cobrancas()
    return {"mensagem": "Job disparado manualmente com sucesso!"}