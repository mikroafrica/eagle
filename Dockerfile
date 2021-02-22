FROM node:12

MAINTAINER info@meshileyaseun.dev

WORKDIR /usr/src/app

COPY package*.json ./

#copy all project files into image
COPY . ./

RUN npm install

RUN npm run build

# Bundle app source
COPY . .

EXPOSE 80
CMD [ "node", "lib/index.js" ]
