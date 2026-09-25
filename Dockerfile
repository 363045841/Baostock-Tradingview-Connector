FROM python:3.12-slim

WORKDIR /app

# 安装 uv
RUN pip install uv

# 复制依赖文件
COPY pyproject.toml uv.lock ./

# 使用 uv 安装依赖
RUN uv sync --frozen --no-dev

# 复制源代码
COPY *.py ./

# 暴露端口
EXPOSE 8000

# 启动服务
CMD ["uv", "run", "python", "-m", "uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]