from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
import os

from database import create_pool, close_pool
from security import get_api_key
from routes import cliente, contrato, parcela, dashboard, Onboarding, adiantamento, extrairpdf
from routes.webhook_btg import router as webhook_btg_router
from scheduler import criar_scheduler, job_cobrancas, job_verificar_pagamentos_btg



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
    
    job_btg = scheduler.get_job("job_pagamentos_btg")
    if job_btg:
        logger.info(f"SISTEMA ONLINE: Monitor BTG 24h ativo — próxima verificação: {job_btg.next_run_time}")
    
    yield
    
    # --- Shutdown ---
    scheduler.shutdown()
    await close_pool()
    logger.info("SISTEMA OFFLINE: Agendador desligado.")

app = FastAPI(
    title="Metropolitan Cobrança API",
    lifespan=lifespan,
)

# --- CORS Seguro ---
allowed_origins_env = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000")
origins_list = [origin.strip() for origin in allowed_origins_env.split(",") if origin.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rotas Protegidas (Exigem x-api-key no header)
protecao = [Depends(get_api_key)]
app.include_router(cliente.router, prefix="/clientes", tags=["Clientes"], dependencies=protecao)
app.include_router(contrato.router, prefix="/contratos", tags=["Contratos"], dependencies=protecao)
app.include_router(parcela.router, prefix="/parcelas", tags=["Parcelas"], dependencies=protecao)
app.include_router(dashboard.router, prefix="/dashboard", tags=["Dashboard"], dependencies=protecao)
app.include_router(Onboarding.router, prefix="/onboarding", tags=["Onboarding"], dependencies=protecao)
app.include_router(adiantamento.router, prefix="/adiantamentos", tags=["Adiantamentos"], dependencies=protecao)
app.include_router(extrairpdf.router, prefix="/api", dependencies=protecao)

# Rotas Públicas / Webhooks (Usam x-btg-signature internamente)
app.include_router(webhook_btg_router, prefix="", tags=["Webhook BTG"])

@app.get("/", tags=["Health"])
async def root():
    job_cob = scheduler.get_job("job_cobrancas")
    job_btg = scheduler.get_job("job_pagamentos_btg")
    return {
        "status": "ok",
        "proximo_cobranca": str(job_cob.next_run_time) if job_cob else None,
        "proximo_btg_check": str(job_btg.next_run_time) if job_btg else None,
    }

@app.post("/admin/disparar-cobrancas", tags=["Admin"], dependencies=[Depends(get_api_key)])
async def disparar_cobrancas_manual():
    await job_cobrancas()
    return {"mensagem": "Job disparado manualmente com sucesso!"}

@app.post("/admin/verificar-pagamentos-btg", tags=["Admin"], dependencies=[Depends(get_api_key)])
async def verificar_pagamentos_btg_manual():
    """Dispara verificação de pagamentos BTG manualmente."""
    await job_verificar_pagamentos_btg()
    return {"mensagem": "Verificação de pagamentos BTG executada!"}