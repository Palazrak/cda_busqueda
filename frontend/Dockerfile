# Usa una imagen oficial de Node.js como base
FROM node:18-alpine

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia package.json y package-lock.json al contenedor
COPY ./frontend/package*.json ./

# Instala las dependencias
RUN npm install sharp && npm install

# Copia el resto del código de la aplicación al contenedor
COPY ./frontend .

# Construye la aplicación Next.js
RUN npm run build

# Expone el puerto que la aplicación usará
EXPOSE 3001

# Comando para iniciar la aplicación
CMD ["npm", "start"]