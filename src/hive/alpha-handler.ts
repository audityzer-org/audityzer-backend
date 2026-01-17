/**
 * Alpha Ceph Hive Mind Handler - Crysis-inspired coordinated audit orchestration
 * Coordinates distributed audits across IoT devices using MQTT pub/sub
 */

import mqtt from 'mqtt';
import Redis from 'ioredis';
import { EventEmitter } from 'events';
import { logger } from '../utils/logger';

interface HiveMessage {
  type: 'audit_request' | 'status_update' | 'threat_detected' | 'mode_switch';
  deviceId: string;
  payload: any;
  timestamp: number;
  signature?: string;
}

interface AlphaCephConfig {
  mqttUrl: string;
  redisUrl: string;
  hiveTopic: string;
}

/**
 * Alpha Ceph - Central coordination node for distributed audits
 * Inspired by Crysis Ceph hive mind collective intelligence
 */
export class AlphaCephHandler extends EventEmitter {
  private mqttClient: mqtt.MqttClient;
  private redis: Redis;
  private slaves: Map<string, { lastSeen: number; mode: string }>;
  private readonly HEARTBEAT_TIMEOUT = 30000; // 30 seconds

  constructor(private config: AlphaCephConfig) {
    super();
    this.slaves = new Map();
    this.redis = new Redis(config.redisUrl);
    this.mqttClient = mqtt.connect(config.mqttUrl);
    this.initializeHive();
  }

  private initializeHive(): void {
    this.mqttClient.on('connect', () => {
      logger.info('🧠 Alpha Ceph hive mind connected');
      this.mqttClient.subscribe(`${this.config.hiveTopic}/#`, (err) => {
        if (err) {
          logger.error('Failed to subscribe to hive topic', err);
        } else {
          logger.info(`Subscribed to ${this.config.hiveTopic}`);
        }
      });
    });

    this.mqttClient.on('message', (topic, message) => {
      this.handleHiveMessage(topic, message);
    });

    // Periodic cleanup of dead slaves
    setInterval(() => this.cleanupDeadSlaves(), this.HEARTBEAT_TIMEOUT);
  }

  private async handleHiveMessage(topic: string, message: Buffer): Promise<void> {
    try {
      const hiveMsg: HiveMessage = JSON.parse(message.toString());
      
      logger.debug(`Received ${hiveMsg.type} from ${hiveMsg.deviceId}`);

      // Update slave status
      this.slaves.set(hiveMsg.deviceId, {
        lastSeen: Date.now(),
        mode: hiveMsg.payload?.mode || 'unknown'
      });

      // Cache in Redis for distributed coordination
      await this.redis.setex(
        `hive:slave:${hiveMsg.deviceId}`,
        60,
        JSON.stringify(hiveMsg)
      );

      switch (hiveMsg.type) {
        case 'audit_request':
          await this.coordinateAudit(hiveMsg);
          break;
        case 'status_update':
          await this.updateSlaveStatus(hiveMsg);
          break;
        case 'threat_detected':
          await this.handleThreatAlert(hiveMsg);
          break;
        case 'mode_switch':
          await this.propagateModeSwitch(hiveMsg);
          break;
      }

      this.emit('hive_message', hiveMsg);
    } catch (error) {
      logger.error('Error handling hive message', error);
    }
  }

  /**
   * Coordinate audit across multiple IoT devices
   */
  private async coordinateAudit(msg: HiveMessage): Promise<void> {
    const { contractAddress, mode } = msg.payload;
    
    logger.info(`🎯 Coordinating audit for ${contractAddress} in ${mode} mode`);

    // Distribute work across all active slaves
    const activeSlaves = Array.from(this.slaves.keys());
    const workload = this.distributeWorkload(contractAddress, activeSlaves);

    for (const [slaveId, task] of workload) {
      this.publishToSlave(slaveId, {
        type: 'audit_task',
        task,
        priority: mode === 'Armor' ? 'high' : 'normal'
      });
    }

    // Cache coordination plan
    await this.redis.setex(
      `hive:audit:${contractAddress}`,
      300,
      JSON.stringify({ slaves: activeSlaves, mode, timestamp: Date.now() })
    );
  }

  /**
   * Handle quantum threat detection alerts
   */
  private async handleThreatAlert(msg: HiveMessage): Promise<void> {
    logger.warn(`⚠️ THREAT DETECTED by ${msg.deviceId}:`, msg.payload);

    // Broadcast to all slaves to switch to Armor mode
    this.broadcastToHive({
      type: 'mode_switch',
      deviceId: 'alpha',
      payload: { mode: 'Armor', reason: 'quantum_threat' },
      timestamp: Date.now()
    });

    // Store threat in Redis for analysis
    await this.redis.lpush(
      'hive:threats',
      JSON.stringify({ ...msg, detectedAt: Date.now() })
    );

    this.emit('quantum_threat', msg.payload);
  }

  /**
   * Propagate mode switch across the hive
   */
  private async propagateModeSwitch(msg: HiveMessage): Promise<void> {
    const { mode, reason } = msg.payload;
    logger.info(`🔄 Propagating mode switch to ${mode} (reason: ${reason})`);

    this.broadcastToHive(msg);
  }

  private async updateSlaveStatus(msg: HiveMessage): Promise<void> {
    const status = msg.payload;
    await this.redis.hset(
      `hive:status:${msg.deviceId}`,
      'energy', status.energy || 0,
      'mode', status.mode || 'unknown',
      'tasks_completed', status.tasksCompleted || 0
    );
  }

  /**
   * Distribute workload using simple round-robin
   */
  private distributeWorkload(target: string, slaves: string[]): Map<string, any> {
    const workload = new Map();
    const chunks = this.splitAuditTasks(target, slaves.length);
    
    slaves.forEach((slaveId, index) => {
      workload.set(slaveId, chunks[index]);
    });

    return workload;
  }

  private splitAuditTasks(target: string, numSlaves: number): any[] {
    // Simplified task splitting - in production would use contract analysis
    return Array(numSlaves).fill(null).map((_, i) => ({
      target,
      partition: i,
      total: numSlaves
    }));
  }

  /**
   * Publish message to specific slave
   */
  private publishToSlave(slaveId: string, payload: any): void {
    this.mqttClient.publish(
      `${this.config.hiveTopic}/slave/${slaveId}`,
      JSON.stringify({ ...payload, from: 'alpha', timestamp: Date.now() })
    );
  }

  /**
   * Broadcast message to all slaves
   */
  private broadcastToHive(message: HiveMessage): void {
    this.mqttClient.publish(
      `${this.config.hiveTopic}/broadcast`,
      JSON.stringify(message)
    );
  }

  /**
   * Remove slaves that haven't sent heartbeat
   */
  private cleanupDeadSlaves(): void {
    const now = Date.now();
    const deadSlaves: string[] = [];

    this.slaves.forEach((info, slaveId) => {
      if (now - info.lastSeen > this.HEARTBEAT_TIMEOUT) {
        deadSlaves.push(slaveId);
      }
    });

    deadSlaves.forEach(slaveId => {
      logger.warn(`💀 Slave ${slaveId} presumed dead - removing from hive`);
      this.slaves.delete(slaveId);
      this.redis.del(`hive:slave:${slaveId}`);
    });
  }

  /**
   * Get hive statistics
   */
  public getHiveStats() {
    return {
      activeSlaves: this.slaves.size,
      slaves: Array.from(this.slaves.entries()).map(([id, info]) => ({
        id,
        mode: info.mode,
        lastSeen: info.lastSeen
      }))
    };
  }

  /**
   * Shutdown hive gracefully
   */
  public async shutdown(): Promise<void> {
    logger.info('Shutting down Alpha Ceph hive mind');
    this.mqttClient.end();
    await this.redis.quit();
  }
}

export default AlphaCephHandler;
