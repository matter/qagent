import axios from "axios";

const client = axios.create({
  baseURL: "/api",
  timeout: 30_000,
});

export default client;
