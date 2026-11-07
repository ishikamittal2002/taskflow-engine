import { app } from './app';

const PORT = process.env.PORT || 4000;

app.listen(PORT, () => {
  console.log(`🚀 TaskFlow Producer API running on http://localhost:${PORT}`);
  console.log(`📊 TaskFlow Queue Dashboard running on http://localhost:${PORT}/admin/queues`);
});