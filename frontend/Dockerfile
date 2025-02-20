# Usa una imagen oficial de Node.js ligera
FROM node:18-alpine

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia solo los archivos de dependencias primero (para aprovechar la caché de Docker)
COPY ./frontend/package*.json ./

# Instala las dependencias de manera optimizada
RUN npm install --frozen-lockfile

# Copia el resto del código de la aplicación
COPY ./frontend . 

# Construye la aplicación Next.js
RUN npm run build

# Expone el puerto que la aplicación usará
EXPOSE 3001

# Comando para iniciar la aplicación
CMD ["npm", "start"]