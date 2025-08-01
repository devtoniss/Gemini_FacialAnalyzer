import os
import json
import logging
import pandas as pd
import boto3
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from botocore.exceptions import ClientError, NoCredentialsError


@dataclass
class ConfiguracaoLambda:
    """Classe para armazenar configurações da Lambda de forma estruturada."""
    dd_api_key: str
    dd_app_key: str
    datadog_api_url: str
    bucket_name: str
    ssl_cert_path: Optional[str]
    object_key: str
    csv_file_path: str
    chunk_size: int = 1000
    max_workers: int = 10
    timeout_requisicao: int = 30


class ProcessadorCSVDatadog:
    """Classe principal para processar CSV e enviar métricas para o Datadog."""
    
    def __init__(self, config: ConfiguracaoLambda):
        self.config = config
        self.logger = self._configurar_logging()
        self.s3_client = self._inicializar_s3_client()
        self.contadores = {
            'sucessos': 0,
            'falhas': 0,
            'total': 0
        }
    
    def _configurar_logging(self) -> logging.Logger:
        """Configura o sistema de logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def _inicializar_s3_client(self) -> boto3.client:
        """Inicializa o cliente S3 com tratamento de erros."""
        try:
            return boto3.client('s3')
        except NoCredentialsError:
            self.logger.error("Credenciais AWS não encontradas")
            raise
        except Exception as e:
            self.logger.error(f"Erro ao inicializar cliente S3: {e}")
            raise
    
    def _validar_configuracao(self) -> bool:
        """Valida se todas as configurações obrigatórias estão presentes."""
        campos_obrigatorios = [
            'dd_api_key', 'dd_app_key', 'datadog_api_url', 
            'bucket_name', 'object_key', 'csv_file_path'
        ]
        
        for campo in campos_obrigatorios:
            valor = getattr(self.config, campo)
            if not valor:
                self.logger.error(f"Configuração obrigatória ausente: {campo}")
                return False
        
        return True
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((ClientError, requests.exceptions.RequestException))
    )
    def _baixar_arquivo_s3(self) -> bool:
        """Baixa o arquivo CSV do S3 com retry automático."""
        try:
            self.logger.info(
                f"Iniciando download do arquivo: s3://{self.config.bucket_name}/{self.config.object_key}"
            )
            
            self.s3_client.download_file(
                self.config.bucket_name,
                self.config.object_key,
                self.config.csv_file_path
            )
            
            self.logger.info("Arquivo CSV baixado com sucesso")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                self.logger.error(f"Arquivo não encontrado no S3: {self.config.object_key}")
            elif error_code == 'NoSuchBucket':
                self.logger.error(f"Bucket não encontrado: {self.config.bucket_name}")
            else:
                self.logger.error(f"Erro do cliente S3: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Erro inesperado ao baixar arquivo: {e}")
            raise
    
    def _validar_estrutura_csv(self, df: pd.DataFrame) -> bool:
        """Valida se o CSV possui as colunas necessárias."""
        colunas_obrigatorias = [
            "arn", "configuration_iops", "account_id", 
            "account_name", "configuration_engine", "configuration_storagetype"
        ]
        
        colunas_ausentes = [col for col in colunas_obrigatorias if col not in df.columns]
        
        if colunas_ausentes:
            self.logger.error(f"Colunas obrigatórias ausentes no CSV: {colunas_ausentes}")
            return False
        
        return True
    
    def _criar_payload_metrica(self, linha: pd.Series, timestamp: int) -> Dict[str, Any]:
        """Cria o payload da métrica para envio ao Datadog."""
        try:
            return {
                "series": [{
                    "metric": "custom.aws.rds.iops.provisioned",
                    "type": 0,
                    "points": [{
                        "timestamp": timestamp,
                        "value": float(linha["configuration_iops"])
                    }],
                    "tags": [
                        f"account_id:{linha['account_id']}",
                        f"account_name:{linha['account_name']}",
                        f"engine:{linha['configuration_engine']}",
                        f"storagetype:{linha['configuration_storagetype']}",
                        f"arn:{linha['arn']}"
                    ]
                }]
            }
        except (KeyError, ValueError, TypeError) as e:
            self.logger.error(f"Erro ao criar payload para linha {linha.name}: {e}")
            raise
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=8),
        retry=retry_if_exception_type(requests.exceptions.RequestException)
    )
    def _enviar_metrica_datadog(self, payload: Dict[str, Any], linha_info: str) -> bool:
        """Envia uma métrica individual para o Datadog com retry."""
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "DD-API-KEY": self.config.dd_api_key,
            "DD-APPLICATION-KEY": self.config.dd_app_key,
        }
        
        try:
            response = requests.post(
                self.config.datadog_api_url,
                json=payload,
                headers=headers,
                timeout=self.config.timeout_requisicao,
                verify=self.config.ssl_cert_path if self.config.ssl_cert_path else True
            )
            
            response.raise_for_status()
            self.logger.debug(f"Métrica enviada com sucesso: {linha_info}")
            return True
            
        except requests.exceptions.Timeout:
            self.logger.error(f"Timeout ao enviar métrica: {linha_info}")
            raise
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"Erro HTTP ao enviar métrica {linha_info}: {e}")
            raise
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Erro de requisição ao enviar métrica {linha_info}: {e}")
            raise
    
    def _processar_linha(self, args: Tuple[int, pd.Series, int]) -> bool:
        """Processa uma linha individual do CSV."""
        indice, linha, timestamp = args
        
        try:
            # Validar dados da linha
            if pd.isna(linha["configuration_iops"]) or linha["configuration_iops"] <= 0:
                self.logger.warning(f"Linha {indice}: IOPS inválido - {linha['configuration_iops']}")
                return False
            
            # Criar payload
            payload = self._criar_payload_metrica(linha, timestamp)
            
            # Enviar métrica
            linha_info = f"ARN: {linha['arn']}"
            sucesso = self._enviar_metrica_datadog(payload, linha_info)
            
            if sucesso:
                self.contadores['sucessos'] += 1
                self.logger.debug(f"Linha {indice} processada com sucesso")
            
            return sucesso
            
        except Exception as e:
            self.contadores['falhas'] += 1
            self.logger.error(f"Erro ao processar linha {indice}: {e}")
            return False
    
    def _processar_chunk(self, chunk: pd.DataFrame, timestamp: int) -> None:
        """Processa um chunk do DataFrame usando ThreadPoolExecutor."""
        # Preparar argumentos para processamento paralelo
        args_lista = [(idx, linha, timestamp) for idx, linha in chunk.iterrows()]
        
        # Processar linhas em paralelo
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submeter todas as tarefas
            futures = {
                executor.submit(self._processar_linha, args): args[0] 
                for args in args_lista
            }
            
            # Aguardar conclusão das tarefas
            for future in as_completed(futures):
                indice = futures[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Erro não tratado na linha {indice}: {e}")
                    self.contadores['falhas'] += 1
    
    def processar_csv_e_enviar_metricas(self) -> Dict[str, int]:
        """Método principal para processar o CSV e enviar métricas."""
        try:
            # Validar configuração
            if not self._validar_configuracao():
                raise ValueError("Configuração inválida")
            
            # Baixar arquivo do S3
            if not self._baixar_arquivo_s3():
                raise RuntimeError("Falha ao baixar arquivo do S3")
            
            # Verificar se arquivo existe localmente
            if not os.path.exists(self.config.csv_file_path):
                raise FileNotFoundError(f"Arquivo CSV não encontrado: {self.config.csv_file_path}")
            
            self.logger.info(f"Iniciando processamento do arquivo: {self.config.csv_file_path}")
            timestamp = int(datetime.now().timestamp())
            
            # Processar CSV em chunks
            try:
                for chunk_num, chunk in enumerate(pd.read_csv(
                    self.config.csv_file_path, 
                    chunksize=self.config.chunk_size
                ), 1):
                    
                    self.logger.info(f"Processando chunk {chunk_num} com {len(chunk)} registros")
                    
                    # Validar estrutura do primeiro chunk
                    if chunk_num == 1 and not self._validar_estrutura_csv(chunk):
                        raise ValueError("Estrutura do CSV inválida")
                    
                    # Limpar dados
                    chunk = chunk.dropna(subset=['arn', 'configuration_iops'])
                    chunk = chunk[chunk['configuration_iops'] > 0]
                    
                    if chunk.empty:
                        self.logger.warning(f"Chunk {chunk_num} vazio após limpeza")
                        continue
                    
                    # Processar chunk
                    self.contadores['total'] += len(chunk)
                    self._processar_chunk(chunk, timestamp)
                    
                    self.logger.info(f"Chunk {chunk_num} processado")
            
            except pd.errors.EmptyDataError:
                self.logger.error("Arquivo CSV está vazio")
                raise
            except pd.errors.ParserError as e:
                self.logger.error(f"Erro ao parsear CSV: {e}")
                raise
            
            # Log do resumo
            self._log_resumo_execucao()
            
            return self.contadores
            
        except Exception as e:
            self.logger.error(f"Erro durante processamento: {e}")
            raise
        finally:
            # Limpeza do arquivo temporário
            self._limpar_arquivo_temporario()
    
    def _log_resumo_execucao(self) -> None:
        """Registra o resumo da execução."""
        self.logger.info("=" * 50)
        self.logger.info("RESUMO DA EXECUÇÃO:")
        self.logger.info(f"Total de registros processados: {self.contadores['total']}")
        self.logger.info(f"Sucessos: {self.contadores['sucessos']}")
        self.logger.info(f"Falhas: {self.contadores['falhas']}")
        
        if self.contadores['total'] > 0:
            taxa_sucesso = (self.contadores['sucessos'] / self.contadores['total']) * 100
            self.logger.info(f"Taxa de sucesso: {taxa_sucesso:.2f}%")
        
        self.logger.info("=" * 50)
    
    def _limpar_arquivo_temporario(self) -> None:
        """Remove o arquivo CSV temporário."""
        try:
            if os.path.exists(self.config.csv_file_path):
                os.remove(self.config.csv_file_path)
                self.logger.info("Arquivo temporário removido com sucesso")
        except Exception as e:
            self.logger.warning(f"Erro ao remover arquivo temporário: {e}")


def obter_configuracao_ambiente() -> ConfiguracaoLambda:
    """Obtém configurações das variáveis de ambiente."""
    return ConfiguracaoLambda(
        dd_api_key=os.getenv("DD_API_KEY", ""),
        dd_app_key=os.getenv("DD_APPLICATION_KEY", ""),
        datadog_api_url=os.getenv("DATADOG_API_URL", ""),
        bucket_name=os.getenv("S3_BUCKET", ""),
        ssl_cert_path=os.getenv("SSL_CERT_FILE"),
        object_key="output/resultado_query.csv",
        csv_file_path="/tmp/resultados_athena.csv",
        chunk_size=int(os.getenv("CHUNK_SIZE", "1000")),
        max_workers=int(os.getenv("MAX_WORKERS", "10")),
        timeout_requisicao=int(os.getenv("REQUEST_TIMEOUT", "30"))
    )


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Função principal da Lambda AWS.
    
    Args:
        event: Evento da Lambda
        context: Contexto da Lambda
    6
    Returns:
        Dict com resultado da execução
    """
    logger = logging.getLogger(__name__)
    logger.info("Iniciando execução da função Lambda...")
    
    try:
        # Obter configurações
        config = obter_configuracao_ambiente()
        
        # Inicializar processador
        processador = ProcessadorCSVDatadog(config)
        
        # Processar CSV e enviar métricas
        resultado = processador.processar_csv_e_enviar_metricas()
        
        # Preparar resposta de sucesso
        resposta = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processamento concluído com sucesso',
                'resultado': resultado,
                'timestamp': datetime.now().isoformat()
            }, ensure_ascii=False)
        }
        
        logger.info("Execução da Lambda concluída com sucesso")
        return resposta
        
    except ValueError as e:
        logger.error(f"Erro de validação: {e}")
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': 'Erro de validação',
                'message': str(e)
            }, ensure_ascii=False)
        }
    
    except FileNotFoundError as e:
        logger.error(f"Arquivo não encontrado: {e}")
        return {
            'statusCode': 404,
            'body': json.dumps({
                'error': 'Arquivo não encontrado',
                'message': str(e)
            }, ensure_ascii=False)
        }
    
    except Exception as e:
        logger.error(f"Erro inesperado: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Erro interno do servidor',
                'message': str(e)
            }, ensure_ascii=False)
        }


# Exemplo de uso local para testes
if __name__ == "__main__":
    # Configurar variáveis de ambiente para teste local
    os.environ.update({
        "DD_API_KEY": "sua_api_key_aqui",
        "DD_APPLICATION_KEY": "sua_app_key_aqui",
        "DATADOG_API_URL": "https://api.datadoghq.com/api/v1/series",
        "S3_BUCKET": "seu_bucket_aqui"
    })
    
    # Executar função
    resultado = lambda_handler({}, None)
    print(json.dumps(resultado, indent=2, ensure_ascii=False))