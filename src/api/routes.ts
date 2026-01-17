import express, { Router } from 'express';
import { AuditOrchestrator } from '../services/audit-orchestrator';

const router: Router = express.Router();
const orchestrator = new AuditOrchestrator();

router.post('/audits', async (req, res) => {
  try {
    const result = await orchestrator.startAudit(req.body);
    res.status(202).json({ auditId: result.auditId, status: 'pending' });
  } catch (error) {
    res.status(500).json({ error: 'Failed to start audit' });
  }
});

router.get('/audits/:auditId', async (req, res) => {
  try {
    const status = await orchestrator.getAuditStatus(req.params.auditId);
    if (!status) return res.status(404).json({ error: 'Not found' });
    res.json(status);
  } catch (error) {
    res.status(500).json({ error: 'Failed' });
  }
});

router.get('/health', (req, res) => {
  res.json({ status: 'healthy' });
});

export default router;
