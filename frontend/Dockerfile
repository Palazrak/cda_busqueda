# Usa una imagen oficial de Node.js ligera
FROM node:18-alpine

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia solo los archivos de dependencias primero (para aprovechar la caché de Docker)
COPY ./frontend/package*.json ./

# Instala las dependencias de manera optimizada
RUN npm ci

# Copia el resto del código de la aplicación
COPY ./frontend . 

# Construye la aplicación Next.js
RUN npm run build

# Next standalone no copia los assets estáticos automáticamente.
# El servidor corre desde .next/standalone y necesita resolver /_next/static ahí.
RUN mkdir -p .next/standalone/.next \
    && cp -R .next/static .next/standalone/.next/static

# Expone el puerto que la aplicación usará
EXPOSE 3001

# Comando para iniciar la aplicación
CMD ["node", ".next/standalone/server.js"]
