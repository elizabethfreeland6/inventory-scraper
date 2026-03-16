# Use the official Apify Node.js base image
FROM apify/actor-node:18

# Copy package files and install dependencies
COPY package*.json ./
RUN npm --quiet set progress=false \
    && npm install --only=prod --no-optional \
    && echo "Dependencies installed"

# Copy the rest of the source code
COPY . ./

# Run the Actor
CMD npm start
