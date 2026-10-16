variable "gcp_project_id" {
  type        = string
  description = "GCP Project ID"
  default     = "gdc4-511816" # Adicione o nome do projeto que sera utilizado para a avaliacao.
}

variable "gcp_region" {
  type        = string
  description = "GCP Region"
  default     = "us-central1"
}

variable "examiner_email" {
  type        = string
  description = "E-mail do examinador para teste de visualizacao de PII"
  default     = "" # Comeca vazio (sem acesso por padrao) e depois coloque o email utilizado na avaliacao para que tenha permissao de acessar os dados PII
}

variable "project_number" {
  type        = string
  description = "Número do projeto GCP (Project Number) para concessão de permissão PII"
  default     = "744078515680"
}