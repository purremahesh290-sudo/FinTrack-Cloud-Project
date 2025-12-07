# Use Node 18 LTS
FROM node:18-alpine

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install --production

COPY . .

ENV PORT=8000
EXPOSE 8000
CMD ["npm", "start"]
