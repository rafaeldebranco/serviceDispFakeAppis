**Agente de resposta para Dispositivo Fake**



-Python
-MQTT

arquivo .ini contem os parametros de configuração





```
web: gunicorn run:run

### Deployment Steps
- Push your code files to a GitHub repository.
- Go to [Railway](https://railway.com), click **New Project**, and select **Deploy from GitHub repo**.
- Choose your repository. Railway auto-detects Python and builds the project.
- Go to your service settings, navigate to **Networking**, and click **Generate Domain** to get your public URL.
