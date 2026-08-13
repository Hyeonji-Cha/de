'''
- 평가를 해야하는 고객 데이터 구조(요쳥/응답)
  - [ {}, {}, ... ]
'''

# 1. 모듈 가져오기
from fastapi import FastAPI
from pydantic import BaseModel  # 요쳥/응답 클레스 구성시 슈퍼클래스 역할
from typing import List         # 요청/응답 데이터 구성시 구조 정의시 사용
import random                   # 신용 평가시 활용

# 2. FastAPI 객체 생성
app = FastAPI

# 3. 요청/응답 구조 정의 -> class
class ReqData(BaseModel):
    # 칼럼 나열
    user_id: str
    income: int     # 소득
    loan_amt: int   # 현재 총 대출액
class ResData(BaseModel):
    # 칼럼 나열
    user_id: str
    credit_score: int   # 0점 - 1000점
    grade: str          # S급, A급, B급, C급...

# 4. 라우팅 : url, 처리함수 매핑 정의
@app.get("/")
def home():
  return {"status":"AI 신용평가 서비스 API"}

@app.post("/predict", response_model=List[ResData]) # 응답 구조 정의
def predict( users:List[ReqData]):  # 요청 구조 정의
  # 고객 1명씩 신용평가하여 => 응답구조 작성후 => 응답
  results = list()
  for user in users: # 고객 1명씩 추출
    '''
      가상 공식
      사전반영식 = (소득//1000) * 10
      credit_score = min( 난수(300, 600) + 사전반영식, 990 )
      grade    = credit_score이 800 이상이면 A, 600 이상이면 B, 나머지 C
    '''
    # 고객 1명 평가
    사전반영식 = (user.income//1000) * 10
    credit_score = min( random.randint(300, 600) + 사전반영식, 990 )
    grade    = "A" if credit_score >= 800 else "B" if credit_score >= 600 else "C"
    # 평가한 고객 정보 담기
    results.append({
      "user_id"       : user.user_id,
      "credit_score"  : credit_score,
      "grade"         : grade
    })

  # 응답
  return results