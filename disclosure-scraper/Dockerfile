FROM apify/actor-node:20

COPY package*.json ./

RUN npm install --only=prod --no-optional --quiet && echo "Dependencies installed"

COPY . ./

CMD npm start --silent
