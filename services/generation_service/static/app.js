const { createApp, nextTick } = Vue;

createApp({
  data() {
    return {
      token: localStorage.getItem("bdn_token") || "",
      user: null,
      authMode: "login",
      authForm: {
        username: "",
        password: ""
      },
      error: "",
      chats: [],
      chatsOffset: 0,
      chatsLimit: 20,
      chatsHasMore: true,
      chatsLoading: false,
      selectedChat: null,
      messages: [],
      messageText: "",
      sendMode: "auto",
      topK: 8,
      sending: false,
      activeEntity: null
    };
  },

  async mounted() {
    if (!this.token) return;
    try {
      await this.fetchMe();
      await this.loadChatsPage(true);
      if (this.chats.length > 0) {
        await this.selectChat(this.chats[0].id);
      }
    } catch (error) {
      console.error(error);
      this.logout();
    }
  },

  methods: {
    authHeaders() {
      return this.token ? { Authorization: `Bearer ${this.token}` } : {};
    },

    parseJsonCandidate(rawText) {
      if (!rawText || typeof rawText !== "string") return null;
      const text = rawText.trim();
      if (!text) return null;

      const candidates = [text];
      const fenced = text.match(/```(?:json)?\s*([\s\S]*?)```/i);
      if (fenced && fenced[1]) candidates.push(fenced[1].trim());
      const braced = text.match(/\{[\s\S]*\}/);
      if (braced && braced[0]) candidates.push(braced[0].trim());

      for (const candidate of candidates) {
        const parsed = this.safeParseJson(candidate);
        if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
          return parsed;
        }
      }
      return null;
    },

    safeParseJson(candidate) {
      try {
        const loaded = JSON.parse(candidate);
        if (typeof loaded === "string") {
          try {
            return JSON.parse(loaded);
          } catch {
            return null;
          }
        }
        return loaded;
      } catch {
        if (candidate.includes('\\"')) {
          try {
            return JSON.parse(candidate.replace(/\\"/g, "\""));
          } catch {
            return null;
          }
        }
        return null;
      }
    },

    normalizeEntityLabel(value) {
      const text = String(value || "").trim();
      if (!text) return "";
      const parts = text.split(".").map((item) => item.trim()).filter(Boolean);
      if (parts.length === 0) return "";

      const sourcePrefixes = new Set([
        "postgres",
        "postgresql",
        "clickhouse",
        "mysql",
        "mariadb",
        "oracle",
        "mssql",
        "mongodb",
        "redis",
        "kafka",
        "s3",
        "minio",
        "bigquery",
        "snowflake"
      ]);
      if (parts.length >= 4 && sourcePrefixes.has(parts[0].toLowerCase())) {
        return parts.slice(1).join(".");
      }
      if (parts.length > 3) {
        return parts.slice(-3).join(".");
      }
      return parts.join(".");
    },

    uniqueEntityList(values) {
      const unique = [];
      for (const item of Array.isArray(values) ? values : []) {
        const label = this.normalizeEntityLabel(item);
        if (label && !unique.includes(label)) unique.push(label);
      }
      return unique;
    },

    normalizeEntityDetailsMap(entityDetails) {
      const result = {};
      if (!entityDetails || typeof entityDetails !== "object" || Array.isArray(entityDetails)) {
        return result;
      }
      for (const [rawKey, rawValue] of Object.entries(entityDetails)) {
        const key = this.normalizeEntityLabel(rawKey);
        if (!key) continue;
        const value = (rawValue && typeof rawValue === "object" && !Array.isArray(rawValue))
          ? rawValue
          : {};
        result[key] = {
          ...(result[key] || {}),
          ...value
        };
      }
      return result;
    },

    getMessageEntities(message) {
      const metadata = (message && message.metadata) ? message.metadata : {};
      const usedEntities = this.uniqueEntityList(metadata.used_entities || []);
      if (usedEntities.length > 0) return usedEntities;
      return this.uniqueEntityList(metadata.sources || []);
    },

    getEntityDetails(message, entity) {
      const metadata = (message && message.metadata) ? message.metadata : {};
      const map = this.normalizeEntityDetailsMap(metadata.entity_details);
      const key = this.normalizeEntityLabel(entity);
      return map[key] || {};
    },

    toggleEntityDetails(messageId, entity) {
      const normalizedEntity = this.normalizeEntityLabel(entity);
      if (!normalizedEntity) return;
      if (
        this.activeEntity &&
        this.activeEntity.messageId === messageId &&
        this.activeEntity.entity === normalizedEntity
      ) {
        this.activeEntity = null;
        return;
      }
      this.activeEntity = { messageId, entity: normalizedEntity };
    },

    isEntityExpanded(messageId, entity) {
      const normalizedEntity = this.normalizeEntityLabel(entity);
      return Boolean(
        this.activeEntity &&
        this.activeEntity.messageId === messageId &&
        this.activeEntity.entity === normalizedEntity
      );
    },

    formatSearchState(value) {
      if (value === true) return "yes";
      if (value === false) return "no";
      return "auto";
    },

    normalizeAssistantContent(message) {
      const normalized = {
        ...message,
        metadata: { ...(message.metadata || {}) }
      };
      if (normalized.role !== "assistant") return normalized;
      if (!normalized.metadata.query && typeof normalized.metadata.sql_query === "string" && normalized.metadata.sql_query.trim()) {
        normalized.metadata.query = normalized.metadata.sql_query.trim();
      }

      const parsed = this.parseJsonCandidate(normalized.content);
      if (!parsed) {
        return normalized;
      }

      if (typeof parsed.answer === "string" && parsed.answer.trim()) {
        normalized.content = parsed.answer.trim();
      }
      if (!normalized.metadata.query && typeof parsed.query === "string" && parsed.query.trim()) {
        normalized.metadata.query = parsed.query.trim();
      }
      if (!normalized.metadata.query && typeof parsed.sql_query === "string" && parsed.sql_query.trim()) {
        normalized.metadata.query = parsed.sql_query.trim();
      }
      if (
        (!Array.isArray(normalized.metadata.used_entities) || normalized.metadata.used_entities.length === 0) &&
        Array.isArray(parsed.used_entities)
      ) {
        normalized.metadata.used_entities = this.uniqueEntityList(parsed.used_entities);
      }
      if (
        (normalized.metadata.limitations === null || normalized.metadata.limitations === undefined || normalized.metadata.limitations === "") &&
        parsed.limitations &&
        parsed.limitations !== "null"
      ) {
        normalized.metadata.limitations = String(parsed.limitations);
      }
      if (
        (!Array.isArray(normalized.metadata.sources) || normalized.metadata.sources.length === 0) &&
        Array.isArray(normalized.metadata.used_entities)
      ) {
        normalized.metadata.sources = this.uniqueEntityList(normalized.metadata.used_entities);
      }
      normalized.metadata.used_entities = this.uniqueEntityList(normalized.metadata.used_entities || []);
      normalized.metadata.sources = this.uniqueEntityList(normalized.metadata.sources || []);
      normalized.metadata.entity_details = this.normalizeEntityDetailsMap(normalized.metadata.entity_details);

      return normalized;
    },

    normalizeMessage(message) {
      const base = {
        ...message,
        metadata: { ...(message.metadata || {}) }
      };
      if (base.role === "assistant") {
        return this.normalizeAssistantContent(base);
      }
      return base;
    },

    async submitAuth() {
      this.error = "";
      try {
        const endpoint = this.authMode === "login"
          ? "/api/auth/login"
          : "/api/auth/register";
        const response = await fetch(endpoint, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(this.authForm)
        });
        const data = await response.json();
        if (!response.ok) {
          this.error = data.detail || "Ошибка авторизации";
          return;
        }
        this.token = data.access_token;
        localStorage.setItem("bdn_token", this.token);
        this.user = data.user;
        this.authForm.password = "";
        await this.loadChatsPage(true);
        if (this.chats.length > 0) {
          await this.selectChat(this.chats[0].id);
        }
      } catch (error) {
        this.error = `Ошибка сети: ${error}`;
      }
    },

    async fetchMe() {
      const response = await fetch("/api/me", { headers: this.authHeaders() });
      if (!response.ok) throw new Error("Unauthorized");
      this.user = await response.json();
    },

    logout() {
      this.token = "";
      this.user = null;
      this.chats = [];
      this.selectedChat = null;
      this.messages = [];
      this.activeEntity = null;
      localStorage.removeItem("bdn_token");
    },

    async loadChatsPage(reset = false) {
      if (this.chatsLoading) return;
      if (!reset && !this.chatsHasMore) return;

      if (reset) {
        this.chats = [];
        this.chatsOffset = 0;
        this.chatsHasMore = true;
      }

      this.chatsLoading = true;
      try {
        const response = await fetch(
          `/api/chats/page?offset=${this.chatsOffset}&limit=${this.chatsLimit}`,
          { headers: this.authHeaders() }
        );
        const data = await response.json();
        if (!response.ok) {
          this.error = data.detail || "Не удалось получить чаты";
          return;
        }

        const items = Array.isArray(data.items) ? data.items : [];
        this.chats = reset ? items : [...this.chats, ...items];
        this.chatsHasMore = Boolean(data.has_more);
        this.chatsOffset = Number.isInteger(data.next_offset)
          ? data.next_offset
          : this.chatsOffset + items.length;

        if (this.selectedChat) {
          const updated = this.chats.find((chat) => chat.id === this.selectedChat.id);
          if (updated) this.selectedChat = updated;
        }
      } catch (error) {
        this.error = `Ошибка сети: ${error}`;
      } finally {
        this.chatsLoading = false;
      }
    },

    onChatListScroll(event) {
      const element = event.target;
      if (!element || this.chatsLoading || !this.chatsHasMore) return;
      const threshold = 100;
      const shouldLoad = element.scrollTop + element.clientHeight >= element.scrollHeight - threshold;
      if (shouldLoad) {
        this.loadChatsPage(false);
      }
    },

    async createChat() {
      const response = await fetch("/api/chats", {
        method: "POST",
        headers: {
          ...this.authHeaders(),
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ title: "Новый чат" })
      });
      const data = await response.json();
      if (!response.ok) {
        this.error = data.detail || "Не удалось создать чат";
        return;
      }
      await this.loadChatsPage(true);
      await this.selectChat(data.id);
    },

    async deleteChat(chatId) {
      const shouldDelete = window.confirm("Удалить чат? Это действие нельзя отменить.");
      if (!shouldDelete) return;

      const wasSelected = this.selectedChat && this.selectedChat.id === chatId;
      try {
        const response = await fetch(`/api/chats/${chatId}`, {
          method: "DELETE",
          headers: this.authHeaders()
        });
        if (!response.ok) {
          let detail = "Не удалось удалить чат";
          try {
            const data = await response.json();
            detail = data.detail || detail;
          } catch {
            // no-op
          }
          throw new Error(detail);
        }

        this.chats = this.chats.filter((chat) => chat.id !== chatId);
        if (wasSelected) {
          this.selectedChat = null;
          this.messages = [];
        }

        if (this.chats.length === 0 && this.chatsHasMore) {
          await this.loadChatsPage(true);
        }

        if (wasSelected && this.chats.length > 0) {
          await this.selectChat(this.chats[0].id);
        }
      } catch (error) {
        this.error = `Ошибка удаления чата: ${error}`;
      }
    },

    async selectChat(chatId) {
      this.error = "";
      this.activeEntity = null;
      this.selectedChat = this.chats.find((chat) => chat.id === chatId) || null;
      if (!this.selectedChat) return;

      const response = await fetch(`/api/chats/${chatId}/messages`, { headers: this.authHeaders() });
      const data = await response.json();
      if (!response.ok) {
        this.error = data.detail || "Не удалось загрузить сообщения";
        return;
      }
      this.messages = (Array.isArray(data) ? data : []).map((message) => this.normalizeMessage(message));
      await this.scrollToBottom();
    },

    handleComposerKeydown(event) {
      if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        this.sendMessage();
      }
    },

    appendLocalMessage(message) {
      this.messages.push(this.normalizeMessage(message));
    },

    updateMessageById(messageId, patch) {
      const index = this.messages.findIndex((item) => item.id === messageId);
      if (index < 0) return;
      const updated = {
        ...this.messages[index],
        ...patch,
        metadata: {
          ...(this.messages[index].metadata || {}),
          ...((patch && patch.metadata) || {})
        }
      };
      this.messages.splice(index, 1, this.normalizeMessage(updated));
    },

    replaceMessageById(messageId, message) {
      const index = this.messages.findIndex((item) => item.id === messageId);
      if (index < 0) {
        this.appendLocalMessage(message);
        return;
      }
      this.messages.splice(index, 1, this.normalizeMessage(message));
    },

    parseSseEvent(rawEvent) {
      let event = "message";
      const dataLines = [];
      for (const line of rawEvent.split("\n")) {
        if (line.startsWith("event:")) {
          event = line.slice(6).trim();
          continue;
        }
        if (line.startsWith("data:")) {
          dataLines.push(line.slice(5).trim());
        }
      }
      let data = {};
      const rawData = dataLines.join("\n");
      if (rawData) {
        try {
          data = JSON.parse(rawData);
        } catch {
          data = { detail: rawData };
        }
      }
      return { event, data };
    },

    async streamAssistantResponse(payload, assistantLocalId) {
      const response = await fetch(`/api/chats/${this.selectedChat.id}/messages/stream`, {
        method: "POST",
        headers: {
          ...this.authHeaders(),
          "Content-Type": "application/json"
        },
        body: JSON.stringify(payload)
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || "Ошибка отправки сообщения");
      }

      if (!response.body) {
        throw new Error("Streaming response body is empty");
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        while (buffer.includes("\n\n")) {
          const separatorIndex = buffer.indexOf("\n\n");
          const rawEvent = buffer.slice(0, separatorIndex).trim();
          buffer = buffer.slice(separatorIndex + 2);
          if (!rawEvent) continue;

          const parsed = this.parseSseEvent(rawEvent);
          if (parsed.event === "ack") {
            continue;
          }
          if (parsed.event === "delta") {
            const deltaText = String(parsed.data.text || "");
            if (deltaText) {
              const current = this.messages.find((item) => item.id === assistantLocalId);
              const nextText = `${current?.content || ""}${deltaText}`;
              this.updateMessageById(assistantLocalId, { content: nextText, streaming: true });
              await this.scrollToBottom();
            }
            continue;
          }
          if (parsed.event === "done") {
            const message = parsed.data.message;
            if (message) {
              this.replaceMessageById(assistantLocalId, message);
              await this.scrollToBottom();
            }
            continue;
          }
          if (parsed.event === "error") {
            throw new Error(parsed.data.detail || "Ошибка генерации");
          }
        }
      }

      const tailEvent = buffer.trim();
      if (tailEvent) {
        const parsed = this.parseSseEvent(tailEvent);
        if (parsed.event === "done" && parsed.data.message) {
          this.replaceMessageById(assistantLocalId, parsed.data.message);
          await this.scrollToBottom();
        }
        if (parsed.event === "error") {
          throw new Error(parsed.data.detail || "Ошибка генерации");
        }
      }
    },

    async sendMessage() {
      if (!this.selectedChat || !this.messageText.trim() || this.sending) return;

      const content = this.messageText.trim();
      const now = new Date().toISOString();
      const localUserId = `local-user-${Date.now()}`;
      const localAssistantId = `local-assistant-${Date.now() + 1}`;

      this.messageText = "";
      this.activeEntity = null;
      this.appendLocalMessage({
        id: localUserId,
        role: "user",
        mode: this.sendMode,
        content,
        metadata: {},
        created_at: now,
        local: true
      });
      this.appendLocalMessage({
        id: localAssistantId,
        role: "assistant",
        mode: this.sendMode,
        content: "",
        metadata: {
          mode: this.sendMode,
          search_performed: this.sendMode === "new_search" ? true : (this.sendMode === "continue" ? false : null),
          top_k: this.topK,
          sources: []
        },
        created_at: now,
        local: true,
        streaming: true
      });

      this.sending = true;
      this.error = "";
      await this.scrollToBottom();
      try {
        await this.streamAssistantResponse(
          {
            content,
            mode: this.sendMode,
            top_k: this.topK
          },
          localAssistantId
        );
        await this.loadChatsPage(true);
        if (this.selectedChat) {
          const updated = this.chats.find((chat) => chat.id === this.selectedChat.id);
          if (updated) this.selectedChat = updated;
        }
      } catch (error) {
        this.error = `Ошибка сети: ${error}`;
        this.updateMessageById(localAssistantId, {
          content: `Ошибка: ${error}`,
          streaming: false,
          metadata: { limitations: String(error) }
        });
      } finally {
        this.sending = false;
        this.updateMessageById(localAssistantId, { streaming: false });
      }
    },

    async scrollToBottom() {
      await nextTick();
      const box = this.$refs.messagesBox;
      if (!box) return;
      box.scrollTop = box.scrollHeight;
    },

    formatDate(value) {
      if (!value) return "";
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return value;
      return date.toLocaleString("ru-RU", { hour12: false });
    }
  }
}).mount("#app");
