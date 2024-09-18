FROM node:20-alpine as builder

WORKDIR /app

COPY package.json ./

RUN npm install

COPY . .

ARG VITE_GIT_CLIENT_URL

ENV VITE_GIT_CLIENT_URL=${VITE_GIT_CLIENT_URL}

RUN npm run build

FROM nginx:stable-alpine

COPY --from=builder /app/dist /usr/share/nginx/html

COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx" , "-g", "daemon off;"]