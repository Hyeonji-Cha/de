# 이미지 생성 파일
# 1. 이미지 출처
FROM python:3.12.12-slim

# 2. 작업 디렉토리 작성
WORKDIR /app

# 3. 패키지 설치
# pydantic: 데이터 유효성 검사(타입), 구조 검사 (데이터 구조)-> validation
# fastapi: backend
# uvicorn : service load, fastapi proxy
RUN pip install fastapi uvicorn pydantic

# 4. 소스 코드 복사
COPY main.py .

# 5. 구동
CMD ["uvicorn","main:app","--host","0.0.0.0","--port","8080"]